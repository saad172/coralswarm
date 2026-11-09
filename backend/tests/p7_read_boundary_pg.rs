//! Actual-role legacy read and server-receipt negative controls. Synthetic only.
mod support;
use coralswarm_backend::retrieval::RetrievalFilter;
use coralswarm_backend::store::{NewMessage, PostgresStore, Store};
use serde_json::json;
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    Executor, PgPool, Row,
};
use std::str::FromStr;
use uuid::Uuid;

async fn run(tenant: bool) {
    let Ok(dsn) = std::env::var("P7_TEST_BASE_URL") else {
        assert!(
            std::env::var_os("REQUIRE_PG_SUITE").is_none(),
            "P7_TEST_BASE_URL required"
        );
        return;
    };
    let opts = PgConnectOptions::from_str(&dsn).unwrap();
    assert!(matches!(opts.get_host(), "localhost" | "127.0.0.1" | "::1"));
    assert_eq!(opts.get_database(), Some("postgres"));
    let admin = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(opts.clone())
        .await
        .unwrap();
    let db = coralswarm_backend::test_db::TestDatabase::new(&opts, if tenant { "p7_read_tenant" } else { "p7_read_consumer" });
    admin
        .execute(format!("CREATE DATABASE {db}").as_str())
        .await
        .unwrap();
    let owner = PgPoolOptions::new()
        .max_connections(3)
        .connect_with(opts.clone().database(&db))
        .await
        .unwrap();
    if tenant {
        sqlx::migrate!("./migrations_tenant")
            .run(&owner)
            .await
            .unwrap();
        support::tenant_role::set_app_role_password_with_retry(&owner).await;
    } else {
        coralswarm_backend::CONTROL_MIGRATOR
            .run(&owner)
            .await
            .unwrap();
        support::app_role::set_app_role_password_with_retry(&owner).await;
    }
    let role = if tenant {
        "coralswarm_tenant_app"
    } else {
        "coralswarm_app"
    };
    let password = if tenant {
        support::tenant_role::APP_ROLE_TEST_PASSWORD
    } else {
        support::app_role::APP_ROLE_TEST_PASSWORD
    };
    let app = PgPoolOptions::new()
        .max_connections(3)
        .connect_with(opts.database(&db).username(role).password(password))
        .await
        .unwrap();
    let privileges:(bool,bool,i64)=sqlx::query_as("SELECT rolsuper,rolbypassrls,(SELECT count(*) FROM pg_tables WHERE schemaname='public' AND tableowner=current_user) FROM pg_roles WHERE rolname=current_user").fetch_one(&app).await.unwrap();
    assert_eq!(privileges, (false, false, 0));
    let (account, user) = if tenant {
        (Uuid::new_v4(), Uuid::new_v4())
    } else {
        let user: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES($1) RETURNING id")
            .bind(format!("p7-{}@test.local", Uuid::new_v4()))
            .fetch_one(&owner)
            .await
            .unwrap();
        let account: Uuid =
            sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES($1) RETURNING id")
                .bind(user)
                .fetch_one(&owner)
                .await
                .unwrap();
        sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'owner')")
            .bind(account)
            .bind(user)
            .execute(&owner)
            .await
            .unwrap();
        (account, user)
    };
    let graph:Uuid=sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'P7 synthetic read fixture','member',$2) RETURNING id").bind(account).bind(user).fetch_one(&owner).await.unwrap();
    let secret = source(&owner, graph, "lost", "Acme DangerOnly retained evidence").await;
    let independent = source(
        &owner,
        graph,
        "other",
        "Acme independent permitted evidence",
    )
    .await;
    let semantic: Uuid = sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model) VALUES($1,$2,0,'fact','Acme DangerOnly retained evidence','source-atoms-v1','deterministic-fixture') RETURNING semantic_atom_id")
        .bind(graph).bind(secret.1).fetch_one(&owner).await.unwrap();
    let passage: Uuid = sqlx::query_scalar("INSERT INTO semantic_atom_passages(graph_id,source_version_id,semantic_atom_id,start_char,end_char,quote) VALUES($1,$2,$3,0,33,'Acme DangerOnly retained evidence') RETURNING passage_id")
        .bind(graph).bind(secret.1).bind(semantic).fetch_one(&owner).await.unwrap();
    let legacy = atom(
        &owner,
        graph,
        "legacy:chunk:0",
        "Genuine unmapped legacy evidence",
    )
    .await;
    let node:Uuid=sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title,body,entity_type) VALUES($1,'entity','Acme',$2,'project') RETURNING node_id").bind(graph).bind(json!({"digest":{"summary":"DangerOnly StoredOnly unsupported stored summary"},"aliases":["DangerOnly","StoredOnly"]})).fetch_one(&owner).await.unwrap();
    sqlx::query("INSERT INTO node_atoms(node_id,atom_id) SELECT $1,unnest($2::uuid[])")
        .bind(node)
        .bind(vec![secret.2, independent.2])
        .execute(&owner)
        .await
        .unwrap();
    let store = PostgresStore::new(app.clone());
    let mut tx = coralswarm_backend::tenancy::scope::begin_tenant_scoped(&app, account, user)
        .await
        .unwrap();
    let conv = store
        .create_conversation_scoped(&mut tx, account, graph, user, "Owner-authored question")
        .await
        .unwrap();
    let msg = store
        .append_message_scoped(
            &mut tx,
            NewMessage {
                conversation_id: conv.id,
                role: "assistant".into(),
                text: "DangerOnly generated answer".into(),
                citations: json!([secret.2]),
                sources: json!([]),
                mode: None,
                timing: json!({"library_support":{"complete":true,"support":[]}}),
                scope_graph_ids: vec![graph],
            },
        )
        .await
        .unwrap();
    store
        .append_message_scoped(
            &mut tx,
            NewMessage {
                conversation_id: conv.id,
                role: "user".into(),
                text: "Owner question".into(),
                citations: json!([]),
                sources: json!([]),
                mode: None,
                timing: json!({}),
                scope_graph_ids: vec![graph],
            },
        )
        .await
        .unwrap();
    sqlx::query("UPDATE conversations SET title='DangerOnly stale generated title' WHERE id=$1")
        .bind(conv.id)
        .execute(&mut *tx)
        .await
        .unwrap();
    assert_eq!(
        store
            .get_conversation_scoped(&mut tx, conv.id)
            .await
            .unwrap()
            .unwrap()
            .title,
        "Owner question"
    );
    let messages = store.list_messages_scoped(&mut tx, conv.id, None, coralswarm_backend::store::CONVERSATION_MESSAGE_PAGE).await.unwrap();
    assert!(
        messages[0].text.contains("Evidence unavailable"),
        "client-like timing cannot certify an answer"
    );
    let fingerprint: String = sqlx::query_scalar("SELECT encode(sha256(convert_to(jsonb_build_array(content_inline,header_len,source_type,source_id,timestamp,created_at)::text,'UTF8')),'hex') FROM atoms WHERE graph_id=$1 AND atom_id=$2").bind(graph).bind(independent.2).fetch_one(&mut *tx).await.unwrap();
    let receipt = json!([{"kind":"source_passage","graph_id":graph,"source_item_id":secret.0,"source_version_id":secret.1,"semantic_atom_id":semantic,"passage_id":passage},{"kind":"legacy_chunk","graph_id":graph,"atom_id":independent.2,"input_sha256":fingerprint}]);
    let shape: bool = sqlx::query_scalar("SELECT library_support_shape($1)")
        .bind(json!([{"kind":"legacy_chunk","graph_id":graph,"atom_id":legacy,"quote":"private raw text must not enter the receipt"}]))
        .fetch_one(&mut *tx).await.unwrap();
    assert!(
        !shape,
        "receipt schema excludes accidental provider/model text"
    );
    let mismatched = Uuid::new_v4();
    sqlx::query("INSERT INTO answer_support_receipts(receipt_id,graph_id,owner_user_id,support,evidence_complete) VALUES($1,$2,$3,$4,true)")
        .bind(mismatched).bind(graph).bind(user).bind(json!([{"kind":"source_passage","graph_id":graph,"source_item_id":independent.0,"source_version_id":secret.1,"semantic_atom_id":semantic,"passage_id":passage}]))
        .execute(&mut *tx).await.unwrap();
    let valid: bool = sqlx::query_scalar("SELECT library_answer_permitted($1)")
        .bind(mismatched)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert!(
        !valid,
        "passage and version cannot be attributed to a different source item"
    );

    sqlx::query("INSERT INTO answer_support_receipts(receipt_id,graph_id,owner_user_id,support,evidence_complete) VALUES($1,$2,$3,$4,true)").bind(msg.id).bind(graph).bind(user).bind(receipt).execute(&mut *tx).await.unwrap();
    assert_eq!(
        store.list_messages_scoped(&mut tx, conv.id, None, coralswarm_backend::store::CONVERSATION_MESSAGE_PAGE).await.unwrap()[0].text,
        "DangerOnly generated answer"
    );
    assert_eq!(
        store
            .list_atoms_page_scoped(&mut tx, graph, 10, 0)
            .await
            .unwrap()
            .len(),
        3
    );
    assert_eq!(
        store
            .source_groups_scoped(&mut tx, graph)
            .await
            .unwrap()
            .iter()
            .map(|g| g.coral_count)
            .sum::<i64>(),
        3
    );
    assert!(
        store
            .get_node_digest_scoped(&mut tx, node)
            .await
            .unwrap()
            .unwrap()
            .1
            .is_none(),
        "unattributed stored digest never becomes model evidence"
    );
    // Permitted linked source text is legitimate lexical context; cached-only
    // terms must still contribute no match even while the source is admitted.
    assert_eq!(store
        .matched_reef_ids_scoped(&mut tx, graph, "DangerOnly", 10, &RetrievalFilter::default())
        .await.unwrap().into_iter().map(|(id, _)| id).collect::<Vec<_>>(),
        vec![node], "admitted literal context remains searchable");
    assert!(
        store
            .matched_reef_ids_scoped(
                &mut tx,
                graph,
                "StoredOnly",
                10,
                &RetrievalFilter::default()
            )
            .await
            .unwrap()
            .is_empty(),
        "stored alias/body cannot influence readable reef matching"
    );
    tx.commit().await.unwrap();
    let superseder:Uuid=sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title,body) VALUES($1,'entity','Genuine','{}') RETURNING node_id").bind(graph).fetch_one(&owner).await.unwrap();
    sqlx::query("INSERT INTO node_atoms(node_id,atom_id) SELECT $1,unnest($2::uuid[])").bind(superseder).bind(vec![secret.2,legacy]).execute(&owner).await.unwrap();
    sqlx::query("INSERT INTO supersession_edge(graph_id,superseded_id,superseder_id,source) VALUES($1,$2,$3,'manual')").bind(graph).bind(node).bind(superseder).execute(&owner).await.unwrap();
    // Dedicated actual-role singleton pool: the unscoped legacy method gets
    // the same synthetic account/actor admission as the scoped path. No
    // production pool adopts these session settings or BYPASSRLS privileges.
    let direct = PgPoolOptions::new().max_connections(1).after_connect(move |conn,_| Box::pin(async move {
        sqlx::query("SELECT set_config('app.account_id',$1,false),set_config('app.user_id',$2,false)").bind(account.to_string()).bind(user.to_string()).execute(conn).await?;
        Ok(())
    })).connect_with(app.connect_options().as_ref().clone()).await.unwrap();
    let direct_store=PostgresStore::new(direct.clone());
    for access in ["restricted", "unknown"] {
        sqlx::query("UPDATE source_items SET access_state=$2 WHERE source_item_id=$1")
            .bind(secret.0)
            .bind(access)
            .execute(&owner)
            .await
            .unwrap();
        let mut tx = coralswarm_backend::tenancy::scope::begin_tenant_scoped(&app, account, user)
            .await
            .unwrap();
        assert!(store
            .atom_detail_scoped(&mut tx, secret.2, account)
            .await
            .unwrap()
            .is_none());
        assert!(store
            .atom_inline_content_scoped(&mut tx, &[secret.2])
            .await
            .unwrap()
            .is_empty());
        assert!(store
            .keyword_search_scoped(
                &mut tx,
                graph,
                "DangerOnly",
                10,
                &RetrievalFilter::default()
            )
            .await
            .unwrap()
            .is_empty());
        // The original denied-term oracle now runs after authoritative support loss.
        assert!(
            store
                .matched_reef_ids_scoped(
                    &mut tx,
                    graph,
                    "DangerOnly",
                    10,
                    &RetrievalFilter::default()
                )
                .await
                .unwrap()
                .is_empty(),
            "stored alias/body cannot influence readable reef matching"
        );
        assert!(
            store
                .matched_reef_ids_scoped(
                    &mut tx,
                    graph,
                    "StoredOnly",
                    10,
                    &RetrievalFilter::default()
                )
                .await
                .unwrap()
                .is_empty(),
            "stored alias/body cannot influence readable reef matching"
        );
        assert_eq!(
            store
                .list_atoms_page_scoped(&mut tx, graph, 10, 0)
                .await
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            store
                .source_groups_scoped(&mut tx, graph)
                .await
                .unwrap()
                .iter()
                .map(|g| g.coral_count)
                .sum::<i64>(),
            2
        );
        let names = store
            .nodes_by_ids_with_counts_scoped(&mut tx, graph, &[node])
            .await
            .unwrap();
        assert_eq!(names[0].1, "Acme");
        assert_eq!(names[0].2, 1, "independent permitted support survives");
        let messages = store.list_messages_scoped(&mut tx, conv.id, None, coralswarm_backend::store::CONVERSATION_MESSAGE_PAGE).await.unwrap();
        assert!(messages[0].text.contains("Evidence unavailable"));
        assert_eq!(messages[0].citations, json!([]));
        assert!(store
            .search_conversations_scoped(&mut tx, graph, user, "DangerOnly", 10)
            .await
            .unwrap()
            .is_empty());
        // Every aggregate dimension uses the same admitted set, including
        // source/window totals and supported node names.
        let stats = store
            .ocean_stats_scoped(account, user, graph)
            .await
            .unwrap();
        assert_eq!(stats.atom_count, 2);
        for dimension in coralswarm_backend::store::dimensions::DIMENSIONS {
            let sql = coralswarm_backend::store::dimensions::build_sql(
                dimension,
                coralswarm_backend::store::dimensions::Dialect::Pg,
            );
            let values: Vec<(String, i64)> = sqlx::query_as(&sql)
                .bind(graph)
                .fetch_all(&mut *tx)
                .await
                .unwrap();
            assert!(values
                .iter()
                .all(|(label, _)| !label.contains("DangerOnly")));
        }
        let history = Uuid::new_v4();
        sqlx::query("INSERT INTO query_history(id,account_id,graph_id,user_id,query_text,answer_summary,citation_count) VALUES($1,$2,$3,$4,'Owner question','DangerOnly stale late history answer',1)")
            .bind(history).bind(account).bind(graph).bind(user).execute(&mut *tx).await.unwrap();
        let rows = store
            .list_query_history_scoped(&mut tx, account, user, Some(graph), 10, 0)
            .await
            .unwrap();
        assert!(rows
            .iter()
            .all(|row| row.answer_summary.is_empty() && row.citation_count == 0));
        assert!(store.expand_scoped(&mut tx,graph,&[secret.2],None).await.unwrap().is_empty(),"denied-only seeds cannot select an independently readable node through a private link");
        let scoped=store.expand_scoped(&mut tx,graph,&[secret.2,independent.2],None).await.unwrap();
        assert_eq!(scoped.len(),1);
        assert_eq!(scoped[0].node_id,node);
        assert!(!scoped[0].atom_ids.contains(&secret.2));
        assert!(scoped[0].atom_ids.contains(&independent.2));
        assert_eq!(scoped[0].injected_atom_ids,vec![legacy],"only independently permitted superseder support is injected");
        tx.commit().await.unwrap();
        assert!(direct_store.expand(graph,&[secret.2],None).await.unwrap().is_empty());
        let direct_rows=direct_store.expand(graph,&[secret.2,independent.2],None).await.unwrap();
        assert_eq!(direct_rows.len(),scoped.len());
        for (a,b) in direct_rows.iter().zip(&scoped) {
            assert_eq!(a.node_id,b.node_id);assert_eq!(a.title,b.title);
            let mut left=a.atom_ids.clone();left.sort();let mut right=b.atom_ids.clone();right.sort();assert_eq!(left,right,"scoped/unscoped support parity");
            assert_eq!(a.injected_atom_ids,b.injected_atom_ids);
        }
    }
    direct.close().await;
    sqlx::query("DELETE FROM source_items WHERE source_item_id=$1")
        .bind(secret.0)
        .execute(&owner)
        .await
        .unwrap();
    let bound: bool =
        sqlx::query_scalar("SELECT source_evidence_bound FROM atoms WHERE atom_id=$1")
            .bind(secret.2)
            .fetch_one(&owner)
            .await
            .unwrap();
    assert!(
        bound,
        "FK cascade does not reclassify a mapped raw chunk as genuine legacy"
    );
    let mut tx = coralswarm_backend::tenancy::scope::begin_tenant_scoped(&app, account, user)
        .await
        .unwrap();
    assert!(store
        .atom_detail_scoped(&mut tx, secret.2, account)
        .await
        .unwrap()
        .is_none());
    assert!(store
        .atom_detail_scoped(&mut tx, legacy, account)
        .await
        .unwrap()
        .is_some());
    assert_eq!(
        store
            .list_atoms_page_scoped(&mut tx, graph, 10, 0)
            .await
            .unwrap()
            .len(),
        2
    );
    tx.commit().await.unwrap();
    let mut tx =
        coralswarm_backend::tenancy::scope::begin_tenant_scoped(&app, account, Uuid::new_v4())
            .await
            .unwrap();
    let allowed: bool = sqlx::query_scalar("SELECT library_answer_permitted($1)")
        .bind(msg.id)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert!(
        !allowed,
        "receipt rechecks private graph reachability for current actor"
    );
    tx.commit().await.unwrap();
    if !tenant {
        assert_private_route_headers(&owner, app.clone(), user, account, legacy).await;
    }
    app.close().await;
    owner.close().await;
    admin
        .execute(format!("DROP DATABASE {db} WITH (FORCE)").as_str())
        .await
        .unwrap();
    admin.close().await;
}
async fn atom(pool: &PgPool, graph: Uuid, source: &str, content: &str) -> Uuid {
    sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,title,excerpt,channel_id,timestamp) VALUES($1,'slack',$2,'fixture-only',$3,$3,$3,'fixture',now()) RETURNING atom_id").bind(graph).bind(source).bind(content).fetch_one(pool).await.unwrap()
}
async fn source(pool: &PgPool, graph: Uuid, id: &str, text: &str) -> (Uuid, Uuid, Uuid) {
    let item:Uuid=sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id) VALUES($1,'slack','fixture:workspace',$2) RETURNING source_item_id").bind(graph).bind(id).fetch_one(pool).await.unwrap();
    let version:Uuid=sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,raw_inline,extraction_version) VALUES($1,$2,$3,'slack','fixture:workspace',$4,'source-atoms-v1') RETURNING version_id").bind(graph).bind(item).bind(id).bind(text).fetch_one(pool).await.unwrap();
    sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
        .bind(item)
        .bind(version)
        .execute(pool)
        .await
        .unwrap();
    let raw = atom(pool, graph, &format!("{id}:chunk:0"), text).await;
    sqlx::query(
        "INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) VALUES($1,$2,$3)",
    )
    .bind(graph)
    .bind(version)
    .bind(raw)
    .execute(pool)
    .await
    .unwrap();
    (item, version, raw)
}
/// Each verdict call in a registered statement is evaluated once per row. The
/// control is the conversation page before #1486: one call in the SQL, pulled
/// up into each of its references, which turned a 0.2 s open into 2 s.
#[tokio::test]
async fn verdict_statements_plan_each_verdict_once() {
    let Ok(dsn) = std::env::var("P7_TEST_BASE_URL") else {
        assert!(std::env::var_os("REQUIRE_PG_SUITE").is_none(), "P7_TEST_BASE_URL required");
        return;
    };
    let opts = PgConnectOptions::from_str(&dsn).unwrap();
    let admin = PgPoolOptions::new().max_connections(1).connect_with(opts.clone()).await.unwrap();
    let db = coralswarm_backend::test_db::TestDatabase::new(&opts, "p7_verdict_plan");
    admin.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
    let owner = PgPoolOptions::new().max_connections(1).connect_with(opts.database(&db)).await.unwrap();
    sqlx::migrate!("./migrations_tenant").run(&owner).await.unwrap();
    let calls = |text: &str, function: &str| {
        text.match_indices(function).filter(|(at, _)| text[at + function.len()..].starts_with('(')).count()
    };
    // EXPLAIN prints a value a lower node computed as `(expr)` wherever an
    // upper node reads it; drop those reads so only evaluations are counted.
    let evaluations = |planned: &str, function: &str| {
        let mut text = planned.to_string();
        for line in planned.lines().filter_map(|l| l.trim().strip_prefix("Output: ")) {
            let (mut depth, mut start) = (0, 0);
            for (i, c) in line.char_indices().chain([(line.len(), ',')]) {
                depth += (c == '(') as i32 - (c == ')') as i32;
                if c == ',' && depth == 0 {
                    let item = line[start..i].trim();
                    if calls(item, function) > 0 {
                        text = text.replace(&format!("({item})"), "");
                    }
                    start = i + 1;
                }
            }
        }
        calls(&text, function)
    };
    let plan = |sql: &str| {
        let owner = owner.clone();
        let sql = format!("EXPLAIN (GENERIC_PLAN, VERBOSE, COSTS OFF) {sql}");
        async move {
            let rows = sqlx::raw_sql(&sql).fetch_all(&owner).await.unwrap();
            rows.iter().map(|row| row.get::<String, _>(0)).collect::<Vec<_>>().join("\n")
        }
    };
    let pulled_up = r#"SELECT id, CASE WHEN support_verified THEN text END, CASE WHEN support_verified THEN citations END, support_verified
        FROM (SELECT page.*, (page.role='user' OR library_answer_permitted(page.id)) AS support_verified
              FROM (SELECT * FROM conversation_messages WHERE conversation_id=$1 ORDER BY seq DESC LIMIT $2) page) recent"#;
    let control = plan(pulled_up).await;
    assert!(evaluations(&control, "library_answer_permitted") > calls(pulled_up, "library_answer_permitted"), "{control}");
    for (name, sql) in coralswarm_backend::store::verdict::STATEMENTS {
        let planned = plan(sql).await;
        let verdicts = coralswarm_backend::store::verdict::VERDICT_FUNCTIONS;
        assert!(verdicts.iter().any(|function| evaluations(&planned, function) > 0), "{name} plans no verdict\n{planned}");
        for function in verdicts {
            assert!(
                evaluations(&planned, function) <= calls(sql, function),
                "{name}: {function} evaluated {} times for {} calls\n{planned}",
                evaluations(&planned, function),
                calls(sql, function)
            );
        }
    }
    // This SQL table function contains one atom verdict. Its inlining makes
    // that verdict appear in a caller plan even when the caller names none.
    // Measure its single-result projection through the actual installed helper;
    // retain a positive control for the legacy projection+filter pull-up.
    let session_once = plan("SELECT sv.session_id FROM library_session_versions($1::uuid) sv WHERE sv.version_id=$2::uuid OFFSET 0").await;
    let session_twice = plan("SELECT sv.session_id FROM library_session_versions($1::uuid) sv WHERE sv.version_id=$2::uuid AND sv.session_id IS NOT NULL").await;
    for function in coralswarm_backend::store::verdict::VERDICT_FUNCTIONS {
        assert_eq!(evaluations(&session_once, function), usize::from(*function == "library_atom_permitted"), "unexpected helper closure: {function}\n{session_once}");
    }
    assert!(evaluations(&session_twice, "library_atom_permitted") > evaluations(&session_once, "library_atom_permitted"), "unfenced session control did not amplify\n{session_twice}");
    // Generated Library programs are absent from the fixed Store registry.
    // Plan all final bodies, including text/scope substitutions. Clean facts
    // can have no direct verdict; the existing registry's nonzero check stays.
    use coralswarm_backend::library_read::{self, LibraryScope};
    let mut generated = 0;
    for mode in ["all_retained", "current"] { for filtered in [false, true] {
        for (strategy, query) in library_read::read_model::DEFINITION_QUERY_VARIANTS {
            let mut shape = serde_json::json!({"graph_ids":[Uuid::nil()],"outcome_versions":mode});
            if filtered { shape["collection"] = serde_json::json!("meetings"); }
            let mut scope: LibraryScope = serde_json::from_value(shape).unwrap();
            scope.normalize().unwrap();
            let program = library_read::read_model::definition_program(&scope, query);
            for stage in ["clean_model_attempt_sql", "dirty_model_attempt_sql", "conditional_live_sql"] {
                let sql = program[stage].as_str().unwrap();
                let planned = plan(sql).await;
                let session_calls = calls(sql, "library_session_versions");
                assert_eq!(session_calls, 2, "one source-page and one outcome-page session projection");
                for function in coralswarm_backend::store::verdict::VERDICT_FUNCTIONS {
                    let inherited = session_calls * evaluations(&session_once, function);
                    assert!(evaluations(&planned, function) <= calls(sql, function) + inherited,
                        "generated {mode}/{filtered}/{strategy}/{stage}: {function} evaluated {} times for {} direct + {inherited} helper calls\n{planned}",
                        evaluations(&planned, function), calls(sql, function));
                }
                generated += 1;
            }
        }
    }}
    assert_eq!(generated, 36);
    owner.close().await;
    admin.execute(format!("DROP DATABASE {db}").as_str()).await.unwrap();
    admin.close().await;
}

#[tokio::test]
async fn consumer_legacy_reads_and_replay_recheck_authoritative_support() {
    run(false).await;
}
#[tokio::test]
async fn tenant_legacy_reads_and_replay_recheck_authoritative_support() {
    run(true).await;
}

async fn assert_private_route_headers(owner: &PgPool, pool: PgPool, user: Uuid, account: Uuid, atom: Uuid) {
    use axum::{
        body::Body,
        http::{header::CACHE_CONTROL, Request, StatusCode},
    };
    use coralswarm_backend::clerk::test_support::{mint, test_jwks, ISSUER};
    use tower::ServiceExt;
    let sub = format!("p7-fixture-{user}");
    sqlx::query("UPDATE users SET clerk_user_id=$2 WHERE id=$1")
        .bind(user)
        .bind(&sub)
        .execute(owner)
        .await
        .unwrap();
    let jwks = test_jwks();
    let server = axum::Router::new().route(
        "/jwks",
        axum::routing::get(move || {
            let body = jwks.clone();
            async move { ([("content-type", "application/json")], body) }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/jwks", listener.local_addr().unwrap());
    let previous = (
        std::env::var_os("CLERK_ISSUER"),
        std::env::var_os("CLERK_JWKS_URL"),
    );
    unsafe {
        std::env::set_var("CLERK_ISSUER", ISSUER);
        std::env::set_var("CLERK_JWKS_URL", url);
    }
    let task = tokio::spawn(async move { axum::serve(listener, server).await.unwrap() });
    let state = std::sync::Arc::new(coralswarm_backend::test_app_state_owned_with_db_and_store(
        pool.clone(), None,
    ));
    let router = coralswarm_backend::test_support_app(state);
    let token = mint(
        json!({"iss":ISSUER,"sub":sub,"exp":(chrono::Utc::now()+chrono::Duration::hours(1)).timestamp()}),
    );
    for (id, status) in [
        (atom, StatusCode::OK),
        (Uuid::new_v4(), StatusCode::NOT_FOUND),
    ] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/atoms/{id}"))
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), status);
        assert_eq!(response.headers().get(CACHE_CONTROL).unwrap(), "no-store");
    }
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/queries/stream")
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response.headers().get(CACHE_CONTROL).unwrap(),
        "no-store",
        "SSE-bound auth failure must not be cacheable"
    );
    let response = router
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        response.headers().get(CACHE_CONTROL).is_none(),
        "public health policy stays separate"
    );
    assert_deferred_coral_binding(owner,pool,account,atom,&token).await;
    task.abort();
    unsafe {
        for (key, value) in [("CLERK_ISSUER", previous.0), ("CLERK_JWKS_URL", previous.1)] {
            if let Some(value) = value {
                std::env::set_var(key, value);
            } else {
                std::env::remove_var(key);
            }
        }
    }
}

async fn assert_deferred_coral_binding(
    owner: &PgPool,
    pool: PgPool,
    account: Uuid,
    existing_atom: Uuid,
    token: &str,
) {
    use axum::{
        body::{to_bytes, Body},
        http::{Request, StatusCode},
        routing::get,
        Router,
    };
    use coralswarm_core::{crypto::encrypt, kek::wrap_dek, r2::R2Client};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use tower::ServiceExt;
    assert!(
        std::env::var("MASTER_KEK_HEX").is_ok_and(|v| v == "07".repeat(32)),
        "fixed public disposable fixture key required"
    );
    let graph: Uuid = sqlx::query_scalar("SELECT graph_id FROM atoms WHERE atom_id=$1")
        .bind(existing_atom)
        .fetch_one(owner)
        .await
        .unwrap();
    sqlx::query("UPDATE accounts SET dek_wrapped=$2 WHERE id=$1")
        .bind(account)
        .bind(wrap_dek(&[7; 32], &[42; 32]).unwrap())
        .execute(owner)
        .await
        .unwrap();
    for action in ["stable", "rewrite", "rebind"] {
        let rewrite = action != "stable";
        let old = source(
            owner,
            graph,
            &format!("blob-old-{action}"),
            "Sensitive old deleted payload",
        )
        .await;
        let replacement = source(
            owner,
            graph,
            &format!("blob-new-{action}"),
            "Permitted replacement payload",
        )
        .await;
        sqlx::query("UPDATE atoms SET content_inline=NULL,content_blob_key=$2 WHERE atom_id=$1")
            .bind(old.2)
            .bind(format!("{graph}/{}", old.2))
            .execute(owner)
            .await
            .unwrap();
        let owner = owner.clone();
        let bytes =
            serde_json::to_vec(&encrypt(b"Sensitive old deleted payload", &[42; 32]).unwrap())
                .unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let observed = count.clone();
        let fake=Router::new().fallback(get(move || {let owner=owner.clone();let bytes=bytes.clone();let observed=observed.clone();async move {
            observed.fetch_add(1,Ordering::SeqCst);
            if rewrite {
                let mut tx=owner.begin().await.unwrap();
                sqlx::query("DELETE FROM source_items WHERE source_item_id=$1").bind(old.0).execute(&mut *tx).await.unwrap();
                if action=="rewrite" {
                sqlx::query("UPDATE atoms SET content_blob_key=$2,header_len=0,source_id='rebound-private-atom' WHERE atom_id=$1").bind(old.2).bind(format!("{graph}/{}",replacement.2)).execute(&mut *tx).await.unwrap();
                }

                sqlx::query("INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) VALUES($1,$2,$3)").bind(graph).bind(replacement.1).bind(old.2).execute(&mut *tx).await.unwrap();
                tx.commit().await.unwrap();
            }
            ([("content-type","application/json")],bytes)
        }}));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, fake).await.unwrap() });
        let r2 = R2Client::new_with_endpoint_override(
            "fixture",
            "fixture-access",
            "fixture-secret",
            "fixture-bucket",
            &endpoint,
        )
        .await;
        let mut state =
            coralswarm_backend::test_app_state_owned_with_db_and_store(pool.clone(), None);
        state.r2 = Some(Arc::new(r2));
        let router = coralswarm_backend::test_support_app(Arc::new(state));
        let response = router
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/atoms/{}", old.2))
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.headers()["cache-control"], "no-store");
        assert_eq!(
            count.load(Ordering::SeqCst),
            1,
            "real encrypted localhost GET required"
        );
        assert_eq!(
            response.status(),
            if rewrite {
                StatusCode::NOT_FOUND
            } else {
                StatusCode::OK
            }
        );
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 100000).await.unwrap()).unwrap();
        if rewrite {
            assert!(!body.to_string().contains("Sensitive old"));
            assert!(body.get("content").is_none());
        } else {
            assert_eq!(body["content"], "Sensitive old deleted payload");
        }
        server.abort();
    }
}

// ── #1490: batched permission counts and the inlined topic-count predicate ──
//
// The statements before #1490, verbatim, as the oracle. Each counted readable
// links with one `library_atom_permitted` call per link, and the topics count
// and page computed `library_node_title` for every candidate.
const OLD_TOPICS_COUNT_SQL: &str = r#"SELECT COUNT(*)::bigint
FROM nodes n
CROSS JOIN LATERAL (SELECT library_node_title(n.graph_id,n.node_id) AS title OFFSET 0) t
WHERE n.graph_id = ANY($1)
  AND n.entity_type IS NOT NULL AND t.title IS NOT NULL
  AND (n.coral_count > 0 OR EXISTS (SELECT 1 FROM node_semantic_atoms sa WHERE sa.node_id=n.node_id AND sa.graph_id=n.graph_id))
  AND ($2::text IS NULL OR n.entity_type = $2)
  AND ($3::text IS NULL OR t.title ILIKE $3 ESCAPE '\')"#;
const OLD_TOPICS_PAGE_SQL: &str = r#"SELECT n.node_id, t.title, n.entity_type, n.graph_id, g.name,
       (SELECT count(*) FROM node_atoms l WHERE l.node_id=n.node_id AND library_atom_permitted(n.graph_id,l.atom_id)), NULL::timestamptz, n.updated_at
FROM nodes n
JOIN graphs g ON g.id = n.graph_id
CROSS JOIN LATERAL (SELECT library_node_title(n.graph_id,n.node_id) AS title OFFSET 0) t
WHERE n.graph_id = ANY($1)
  AND n.entity_type IS NOT NULL AND t.title IS NOT NULL
  AND (n.coral_count > 0 OR EXISTS (SELECT 1 FROM node_semantic_atoms sa WHERE sa.node_id=n.node_id AND sa.graph_id=n.graph_id))
  AND ($2::text IS NULL OR n.entity_type = $2)
  AND ($3::text IS NULL OR t.title ILIKE $3 ESCAPE '\')
ORDER BY n.updated_at DESC, n.node_id DESC
LIMIT $4 OFFSET $5"#;
const OLD_NODE_TITLES_WITH_COUNTS_SQL: &str = r#"SELECT n.node_id, t.title, (SELECT count(*) FROM node_atoms l WHERE l.node_id=n.node_id AND library_atom_permitted(n.graph_id,l.atom_id)) FROM nodes n
CROSS JOIN LATERAL (SELECT library_node_title(n.graph_id,n.node_id) AS title OFFSET 0) t
WHERE n.graph_id = $1 AND n.node_id = ANY($2) AND t.title IS NOT NULL"#;
const OLD_NODE_DIGEST_CONTEXT_SQL: &str = r#"SELECT n.graph_id, n.type, t.title, (SELECT count(*) FROM node_atoms l WHERE l.node_id=n.node_id AND library_atom_permitted(n.graph_id,l.atom_id)), 0::bigint, NULL::text
FROM nodes n
CROSS JOIN LATERAL (SELECT library_node_title(n.graph_id,n.node_id) AS title OFFSET 0) t
WHERE n.node_id = $1 AND t.title IS NOT NULL"#;
const OLD_NODE_ADMISSION_SQL: &str = "SELECT library_node_title(graph_id,node_id), (SELECT count(*) FROM node_atoms l WHERE l.node_id=n.node_id AND library_atom_permitted(n.graph_id,l.atom_id)) FROM nodes n WHERE n.graph_id=$1 AND n.node_id=$2 AND library_node_permitted(n.graph_id,n.node_id)";
const OLD_LIST_NODES_SQL: &str = "SELECT n.node_id, n.type, COALESCE(library_node_title(n.graph_id,n.node_id),''), n.scope, n.created_at, n.updated_at, \
                            (SELECT count(*) FROM node_atoms permitted_link WHERE permitted_link.node_id=n.node_id AND library_atom_permitted(n.graph_id,permitted_link.atom_id)) AS atom_count \
                     FROM nodes n \
                     WHERE n.graph_id = $1 AND library_node_permitted(n.graph_id,n.node_id) \
                     ORDER BY CASE n.type WHEN 'entity' THEN 0 WHEN 'thread' THEN 1 \
                              WHEN 'decision' THEN 2 WHEN 'reference' THEN 3 ELSE 4 END, \
                              atom_count DESC, n.updated_at DESC, n.node_id ASC \
                     LIMIT $2 OFFSET $3";
const OLD_LIST_NODES_OF_TYPE_SQL: &str = "SELECT n.node_id, n.type, COALESCE(library_node_title(n.graph_id,n.node_id),''), n.scope, n.created_at, n.updated_at, \
                            (SELECT count(*) FROM node_atoms permitted_link WHERE permitted_link.node_id=n.node_id AND library_atom_permitted(n.graph_id,permitted_link.atom_id)) AS atom_count \
                     FROM nodes n \
                     WHERE n.graph_id = $1 AND library_node_permitted(n.graph_id,n.node_id) \
                       AND n.type = $2 \
                     ORDER BY atom_count DESC, n.updated_at DESC, n.node_id ASC \
                     LIMIT $3 OFFSET $4";
const OLD_NODE_EVIDENCE_COUNTS_SQL: &str = r#"
        WITH requested AS (SELECT node_id,graph_id FROM nodes WHERE graph_id=ANY($1) AND node_id=ANY($2) AND library_node_permitted(graph_id,node_id)),
        semantic AS (SELECT na.node_id,na.semantic_atom_id,a.source_item_id FROM requested n
            JOIN node_semantic_atoms na ON na.node_id=n.node_id AND na.graph_id=n.graph_id
            JOIN served_semantic_atoms a ON a.semantic_atom_id=na.semantic_atom_id AND a.graph_id=na.graph_id),
        sources AS (SELECT node_id,source_item_id FROM semantic UNION
            SELECT na.node_id,v.source_item_id FROM requested n
            JOIN node_atoms na ON na.node_id=n.node_id
            JOIN source_version_chunks c ON c.atom_id=na.atom_id AND c.graph_id=n.graph_id
            JOIN source_versions v ON v.version_id=c.source_version_id AND v.graph_id=c.graph_id
            JOIN source_items si ON si.source_item_id=v.source_item_id AND si.graph_id=v.graph_id WHERE si.access_state='permitted' AND library_atom_permitted(n.graph_id,na.atom_id))
        SELECT n.node_id,(SELECT count(*) FROM semantic s WHERE s.node_id=n.node_id) AS semantic_atom_count,
            (SELECT count(*) FROM sources s WHERE s.node_id=n.node_id) AS source_support_count,
            (SELECT count(*) FROM node_atoms na WHERE na.node_id=n.node_id AND library_atom_permitted(n.graph_id,na.atom_id) AND NOT EXISTS
                (SELECT 1 FROM source_version_chunks c WHERE c.graph_id=n.graph_id AND c.atom_id=na.atom_id)) AS unmapped_legacy_chunk_count FROM requested n
    "#;

type TopicRow = (
    Uuid,
    String,
    String,
    Uuid,
    String,
    i64,
    Option<chrono::DateTime<chrono::Utc>>,
    chrono::DateTime<chrono::Utc>,
);
type ListRow = (
    Uuid,
    String,
    String,
    String,
    chrono::DateTime<chrono::Utc>,
    chrono::DateTime<chrono::Utc>,
    i64,
);

/// The ids of the batching fixture's nodes, by what each one exercises.
struct BatchingFixture {
    graph: Uuid,
    other: Uuid,
    account: Uuid,
    user: Uuid,
    falcon: Uuid,
    semantic: Uuid,
    zephyr: Uuid,
    ada: Uuid,
    hub: Uuid,
    other_falcon: Uuid,
    pages: Vec<Uuid>,
    hidden: Vec<(&'static str, Uuid)>,
    all: Vec<Uuid>,
}

async fn reef(pool: &PgPool, graph: Uuid, title: &str, entity_type: Option<&str>, atoms: &[Uuid]) -> Uuid {
    let id: Uuid = sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title,body,entity_type) VALUES($1,'entity',$2,'{}',$3) RETURNING node_id")
        .bind(graph).bind(title).bind(entity_type).fetch_one(pool).await.unwrap();
    sqlx::query("INSERT INTO node_atoms(node_id,atom_id) SELECT $1,unnest($2::uuid[])")
        .bind(id).bind(atoms).execute(pool).await.unwrap();
    id
}

async fn restrict_source_id(pool: &PgPool, graph: Uuid, source_id: &str) {
    let item: Uuid = sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id) VALUES($1,'slack','fixture:workspace',$2) RETURNING source_item_id")
        .bind(graph).bind(source_id).fetch_one(pool).await.unwrap();
    sqlx::query("UPDATE source_items SET access_state='restricted' WHERE source_item_id=$1")
        .bind(item).execute(pool).await.unwrap();
}

async fn semantic_link(pool: &PgPool, graph: Uuid, version: Uuid, ordinal: i32, mention: &str, node: Uuid) {
    let atom: Uuid = sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model,entities) VALUES($1,$2,$3,'fact',$4,'source-atoms-v1','deterministic-fixture',$5) RETURNING semantic_atom_id")
        .bind(graph).bind(version).bind(ordinal).bind(format!("statement about {mention}"))
        .bind(json!([{"mention": mention}])).fetch_one(pool).await.unwrap();
    sqlx::query("INSERT INTO node_semantic_atoms(node_id,semantic_atom_id,graph_id) VALUES($1,$2,$3)")
        .bind(node).bind(atom).bind(graph).execute(pool).await.unwrap();
}

/// An active identity correction naming the node's own title, supported by a
/// legacy (unbound) atom, recorded through the integrity trigger's own checks.
async fn correction(pool: &PgPool, account: Uuid, user: Uuid, graph: Uuid, node: Uuid, label: &str, support: Uuid) {
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("SELECT set_config('app.account_id',$1,true),set_config('app.user_id',$2,true),set_config('app.tenant_actor','member',true)")
        .bind(account.to_string()).bind(user.to_string()).execute(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO reef_identity_corrections(graph_id,node_id,label,recorded_by,support_atom_id,support_kind,content_revision,input_sha256) SELECT $1,$2,$3,$4,a.atom_id,'legacy',a.content_revision,reef_identity_atom_fingerprint(a) FROM atoms a WHERE a.atom_id=$5")
        .bind(graph).bind(node).bind(label).bind(user).bind(support).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
}

/// Mixed permitted and forbidden support across every branch of the title
/// verdict and of `library_permitted_atoms`, plus a hub above the id-list
/// threshold and 30 visible pages that sort first.
async fn batching_fixture(owner: &PgPool) -> BatchingFixture {
    let (account, user) = (Uuid::new_v4(), Uuid::new_v4());
    let graph: Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'Batching fixture','member',$2) RETURNING id").bind(account).bind(user).fetch_one(owner).await.unwrap();
    let other: Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'Batching fixture other','member',$2) RETURNING id").bind(account).bind(user).fetch_one(owner).await.unwrap();
    // Bound atoms: `ok` permitted, `lost` restricted afterwards.
    let ok = source(owner, graph, "ok", "Acme ships Falcon").await;
    let lost = source(owner, graph, "lost", "Falcon Secret plans").await;
    // Unbound atoms: no source item (permitted), or a restricted item with the
    // atom's chunk-stripped source id (denied, tenant 0161).
    let free = atom(owner, graph, "free-notes", "free Falcon notes").await;
    let gone = atom(owner, graph, "gone:chunk:3", "Gone evidence").await;
    let retracted = atom(owner, graph, "retracted-notes", "Retracted evidence").await;
    let zephyr_support = atom(owner, graph, "zephyr-notes", "unrelated words").await;
    let zephyr_denied_support = atom(owner, graph, "zd:chunk:1", "other words").await;
    let ada_atom = atom(owner, graph, "ada-notes", "a meeting").await;
    sqlx::query("UPDATE atoms SET participants=ARRAY['ada'] WHERE atom_id=$1").bind(ada_atom).execute(owner).await.unwrap();

    let falcon = reef(owner, graph, "Falcon", Some("topic"), &[ok.2, lost.2, free]).await;
    let secret = reef(owner, graph, "Secret", Some("topic"), &[lost.2]).await;
    let gone_reef = reef(owner, graph, "Gone", Some("topic"), &[gone]).await;
    let nomatch = reef(owner, graph, "Nomatch", Some("topic"), &[ok.2, free]).await;
    let retracted_reef = reef(owner, graph, "Retracted", Some("topic"), &[retracted]).await;
    let semantic = reef(owner, graph, "Semantic", Some("person"), &[lost.2]).await;
    let semantic_denied = reef(owner, graph, "SemDenied", Some("person"), &[]).await;
    let zephyr = reef(owner, graph, "Zephyr", Some("topic"), &[zephyr_support]).await;
    let zephyr_denied = reef(owner, graph, "Zephyrdenied", Some("topic"), &[zephyr_denied_support]).await;
    let ada = reef(owner, graph, "Ada", Some("person"), &[ada_atom]).await;
    let untyped_atom = atom(owner, graph, "untyped-notes", "the Falcon untyped story").await;
    let untyped = reef(owner, graph, "Falcon untyped", None, &[ok.2, free, untyped_atom]).await;
    // (graph, type, lower(title)) is unique, so the unsupported node has its own title.
    let uncounted = reef(owner, graph, "Orphan", Some("topic"), &[]).await;
    semantic_link(owner, graph, ok.1, 0, "semantic", semantic).await;
    semantic_link(owner, graph, lost.1, 0, "semdenied", semantic_denied).await;
    correction(owner, account, user, graph, zephyr, "Zephyr", zephyr_support).await;
    correction(owner, account, user, graph, zephyr_denied, "Zephyrdenied", zephyr_denied_support).await;

    // 6000 permitted and 6000 denied links: above PERMITTED_IDS_MAX, so the
    // whole-graph permitted set answers it.
    sqlx::query("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,title,excerpt,channel_id,timestamp) SELECT $1,'slack',p||i,'fixture-only','Hub item '||i,'t','e','fixture',now() FROM generate_series(1,6000) i CROSS JOIN (VALUES ('hubok-'),('hubno:chunk:')) v(p)")
        .bind(graph).execute(owner).await.unwrap();
    let hub = reef(owner, graph, "Hub", Some("project"), &[]).await;
    sqlx::query("INSERT INTO node_atoms(node_id,atom_id) SELECT $1,atom_id FROM atoms WHERE graph_id=$2 AND source_id LIKE 'hub%'")
        .bind(hub).bind(graph).execute(owner).await.unwrap();
    assert!(12_000 > coralswarm_backend::store::verdict::PERMITTED_IDS_MAX);

    let mut pages = Vec::new();
    for k in 1..=30 {
        let a = atom(owner, graph, &format!("page-{k}"), &format!("Page {k:02} body")).await;
        pages.push(reef(owner, graph, &format!("Page {k:02}"), Some("topic"), &[a]).await);
    }
    // A second graph: its own permitted support, and a link to an atom of the
    // first graph, which neither the old nor the new count admits.
    let other_atom = atom(owner, other, "other-falcon", "Falcon elsewhere").await;
    let other_falcon = reef(owner, other, "Falcon", Some("topic"), &[other_atom, ok.2]).await;

    // Deny only after the corrections are recorded with valid support.
    sqlx::query("UPDATE source_items SET access_state='restricted' WHERE source_item_id=$1").bind(lost.0).execute(owner).await.unwrap();
    restrict_source_id(owner, graph, "gone").await;
    restrict_source_id(owner, graph, "zd").await;
    restrict_source_id(owner, graph, "hubno").await;
    sqlx::query("UPDATE atoms SET evidence_retracted=true WHERE atom_id=$1").bind(retracted).execute(owner).await.unwrap();

    // Pages sort first, newest first; every other node ties on updated_at,
    // so node_id decides their order.
    sqlx::query("UPDATE nodes SET updated_at='2026-01-01T00:00:00Z' WHERE graph_id=ANY($1)").bind(vec![graph, other]).execute(owner).await.unwrap();
    for (k, page) in pages.iter().enumerate() {
        sqlx::query("UPDATE nodes SET updated_at='2026-02-01T00:00:00Z'::timestamptz - make_interval(mins => $2) WHERE node_id=$1")
            .bind(page).bind(k as i32).execute(owner).await.unwrap();
    }
    let hidden = vec![
        ("forbidden bound support", secret),
        ("denied unbound support", gone_reef),
        ("permitted support without the title", nomatch),
        ("retracted support", retracted_reef),
        ("restricted semantic mention", semantic_denied),
        ("correction on denied support", zephyr_denied),
        ("no support at all", uncounted),
    ];
    let mut all = vec![falcon, semantic, zephyr, ada, hub, untyped];
    all.extend(hidden.iter().map(|(_, id)| *id));
    all.extend(&pages);
    BatchingFixture { graph, other, account, user, falcon, semantic, zephyr, ada, hub, other_falcon, pages, hidden, all }
}

/// #1490. Every statement whose per-link `library_atom_permitted` became one
/// `library_permitted_atoms` call, and the topics count that no longer calls
/// `library_node_title`, returns exactly what the statement it replaced
/// returned, as the tenant app role under RLS, on mixed permitted and
/// forbidden support. Not vacuous: the fixture's expected visibility and
/// counts are asserted first.
#[tokio::test]
async fn library_permission_batching_matches_per_row_sql() {
    let Ok(dsn) = std::env::var("P7_TEST_BASE_URL") else {
        assert!(std::env::var_os("REQUIRE_PG_SUITE").is_none(), "P7_TEST_BASE_URL required");
        return;
    };
    let opts = PgConnectOptions::from_str(&dsn).unwrap();
    assert!(matches!(opts.get_host(), "localhost" | "127.0.0.1" | "::1"));
    let admin = PgPoolOptions::new().max_connections(1).connect_with(opts.clone()).await.unwrap();
    let db = coralswarm_backend::test_db::TestDatabase::new(&opts, "p7_batching");
    admin.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
    let owner = PgPoolOptions::new().max_connections(2).connect_with(opts.clone().database(&db)).await.unwrap();
    sqlx::migrate!("./migrations_tenant").run(&owner).await.unwrap();
    support::tenant_role::set_app_role_password_with_retry(&owner).await;
    let app = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(opts.database(&db).username("coralswarm_tenant_app").password(support::tenant_role::APP_ROLE_TEST_PASSWORD))
        .await
        .unwrap();
    let f = batching_fixture(&owner).await;
    use coralswarm_backend::source_evidence::NODE_EVIDENCE_COUNTS_SQL;
    use coralswarm_backend::store::library::{TOPICS_COUNT_SQL, TOPICS_PAGE_SQL};
    use coralswarm_backend::store::verdict;
    let mut tx = coralswarm_backend::tenancy::scope::begin_tenant_scoped(&app, f.account, f.user).await.unwrap();

    // The fixture means what it says.
    let counted: Vec<(Uuid, String, i64)> = sqlx::query_as(verdict::NODE_TITLES_WITH_COUNTS_SQL)
        .bind(f.graph).bind(&f.all).fetch_all(&mut *tx).await.unwrap();
    let count_of = |id: Uuid| counted.iter().find(|r| r.0 == id).map(|r| r.2);
    assert_eq!(count_of(f.falcon), Some(2), "ok + free, not lost");
    assert_eq!(count_of(f.semantic), Some(0), "titled by its semantic mention; its only link is forbidden");
    assert_eq!(count_of(f.zephyr), Some(1), "titled by its correction");
    assert_eq!(count_of(f.ada), Some(1), "titled by a participant");
    assert_eq!(count_of(f.hub), Some(6000), "whole-graph branch admits only the permitted half");
    for (why, id) in &f.hidden {
        assert_eq!(count_of(*id), None, "{why}");
    }
    let other: Vec<(Uuid, String, i64)> = sqlx::query_as(verdict::NODE_TITLES_WITH_COUNTS_SQL)
        .bind(f.other).bind(vec![f.other_falcon]).fetch_all(&mut *tx).await.unwrap();
    assert_eq!(other, vec![(f.other_falcon, "Falcon".to_string(), 1)], "a link into another graph is not counted");

    // Merge-review titles and counts: below and above the id-list threshold,
    // empty, unknown and cross-graph ids.
    let mut id_sets = vec![
        f.all.clone(),
        vec![f.falcon, f.zephyr, f.hidden[0].1],
        vec![f.hub],
        vec![],
        vec![Uuid::new_v4(), f.other_falcon],
    ];
    id_sets.push(f.all.iter().copied().filter(|id| *id != f.hub).collect());
    for graph in [f.graph, f.other] {
        for ids in &id_sets {
            let mut old: Vec<(Uuid, String, i64)> = sqlx::query_as(OLD_NODE_TITLES_WITH_COUNTS_SQL).bind(graph).bind(ids).fetch_all(&mut *tx).await.unwrap();
            let mut new: Vec<(Uuid, String, i64)> = sqlx::query_as(verdict::NODE_TITLES_WITH_COUNTS_SQL).bind(graph).bind(ids).fetch_all(&mut *tx).await.unwrap();
            old.sort();
            new.sort();
            assert_eq!(old, new, "node titles with counts, graph {graph}, ids {ids:?}");
        }
    }

    // Single-node reads: digest context and the node read DTO.
    type Digest = (Uuid, String, String, i64, i64, Option<String>);
    let mut every = f.all.clone();
    every.extend([f.other_falcon, Uuid::new_v4()]);
    for id in &every {
        let old: Option<Digest> = sqlx::query_as(OLD_NODE_DIGEST_CONTEXT_SQL).bind(id).fetch_optional(&mut *tx).await.unwrap();
        let new: Option<Digest> = sqlx::query_as(verdict::NODE_DIGEST_CONTEXT_SQL).bind(id).fetch_optional(&mut *tx).await.unwrap();
        assert_eq!(old, new, "digest context {id}");
        for graph in [f.graph, f.other] {
            let old: Option<(Option<String>, i64)> = sqlx::query_as(OLD_NODE_ADMISSION_SQL).bind(graph).bind(id).fetch_optional(&mut *tx).await.unwrap();
            let new: Option<(Option<String>, i64)> = sqlx::query_as(verdict::NODE_ADMISSION_SQL).bind(graph).bind(id).fetch_optional(&mut *tx).await.unwrap();
            assert_eq!(old, new, "node admission {graph} {id}");
        }
    }

    // Evidence counts for the topics page and node read.
    for graphs in [vec![f.graph], vec![f.graph, f.other], vec![f.other]] {
        for ids in [every.clone(), vec![f.falcon, f.semantic], vec![f.hub]] {
            let mut old: Vec<(Uuid, i64, i64, i64)> = sqlx::query_as(OLD_NODE_EVIDENCE_COUNTS_SQL).bind(&graphs).bind(&ids).fetch_all(&mut *tx).await.unwrap();
            let mut new: Vec<(Uuid, i64, i64, i64)> = sqlx::query_as(NODE_EVIDENCE_COUNTS_SQL).bind(&graphs).bind(&ids).fetch_all(&mut *tx).await.unwrap();
            old.sort();
            new.sort();
            assert_eq!(old, new, "evidence counts {graphs:?} {ids:?}");
        }
    }

    // Topics count and page, in order, across graphs, kinds, title filters
    // and windows (including past the end).
    for graphs in [vec![f.graph], vec![f.graph, f.other], vec![f.other]] {
        for kind in [None, Some("topic"), Some("person")] {
            for like in [None, Some("%falcon%"), Some("%page 1%"), Some("%zz%")] {
                let old: i64 = sqlx::query_scalar(OLD_TOPICS_COUNT_SQL).bind(&graphs).bind(kind).bind(like).fetch_one(&mut *tx).await.unwrap();
                let new: i64 = sqlx::query_scalar(TOPICS_COUNT_SQL).bind(&graphs).bind(kind).bind(like).fetch_one(&mut *tx).await.unwrap();
                assert_eq!(old, new, "topics count {graphs:?} {kind:?} {like:?}");
                for (limit, offset) in [(5i64, 0i64), (5, 5), (100, 0), (7, 30), (10, 1000)] {
                    let old: Vec<TopicRow> = sqlx::query_as(OLD_TOPICS_PAGE_SQL).bind(&graphs).bind(kind).bind(like).bind(limit).bind(offset).fetch_all(&mut *tx).await.unwrap();
                    let new: Vec<TopicRow> = sqlx::query_as(TOPICS_PAGE_SQL).bind(&graphs).bind(kind).bind(like).bind(limit).bind(offset).fetch_all(&mut *tx).await.unwrap();
                    assert_eq!(old, new, "topics page {graphs:?} {kind:?} {like:?} {limit} {offset}");
                }
            }
        }
    }
    let total: i64 = sqlx::query_scalar(TOPICS_COUNT_SQL).bind(vec![f.graph]).bind(None::<&str>).bind(None::<&str>).fetch_one(&mut *tx).await.unwrap();
    assert_eq!(total, 35, "falcon, semantic, zephyr, ada, hub and 30 pages");
    let page: Vec<TopicRow> = sqlx::query_as(TOPICS_PAGE_SQL).bind(vec![f.graph]).bind(None::<&str>).bind(None::<&str>).bind(100i64).bind(0i64).fetch_all(&mut *tx).await.unwrap();
    assert_eq!(page.iter().take(30).map(|r| r.0).collect::<Vec<_>>(), f.pages, "pages first, newest first");
    for (why, id) in &f.hidden {
        assert!(!page.iter().any(|r| r.0 == *id), "{why}");
    }
    tx.commit().await.unwrap();

    // Legacy unscoped list: same actor admission on a dedicated pool.
    let (account, user) = (f.account, f.user);
    let direct = PgPoolOptions::new().max_connections(1).after_connect(move |conn, _| Box::pin(async move {
        sqlx::query("SELECT set_config('app.account_id',$1,false),set_config('app.user_id',$2,false)").bind(account.to_string()).bind(user.to_string()).execute(conn).await?;
        Ok(())
    })).connect_with(app.connect_options().as_ref().clone()).await.unwrap();
    for (limit, offset) in [(10i64, 0i64), (50, 3), (100, 0)] {
        let old: Vec<ListRow> = sqlx::query_as(OLD_LIST_NODES_SQL).bind(f.graph).bind(limit).bind(offset).fetch_all(&direct).await.unwrap();
        let new: Vec<ListRow> = sqlx::query_as(verdict::LIST_NODES_SQL).bind(f.graph).bind(limit).bind(offset).fetch_all(&direct).await.unwrap();
        assert_eq!(old, new, "list nodes {limit} {offset}");
        assert!(!new.is_empty());
        for kind in ["entity", "decision"] {
            let old: Vec<ListRow> = sqlx::query_as(OLD_LIST_NODES_OF_TYPE_SQL).bind(f.graph).bind(kind).bind(limit).bind(offset).fetch_all(&direct).await.unwrap();
            let new: Vec<ListRow> = sqlx::query_as(verdict::LIST_NODES_OF_TYPE_SQL).bind(f.graph).bind(kind).bind(limit).bind(offset).fetch_all(&direct).await.unwrap();
            assert_eq!(old, new, "list nodes of type {kind} {limit} {offset}");
        }
    }
    direct.close().await;
    app.close().await;
    owner.close().await;
    admin.execute(format!("DROP DATABASE {db} WITH (FORCE)").as_str()).await.unwrap();
    admin.close().await;
}

/// #1490 shape pins. The topics page computes `library_node_title` only for
/// the candidates it reads until the page fills (the `OFFSET 0` fence), the
/// count computes it for none, the count's permission probe is a hashed
/// subplan, and the function bodies `TOPICS_COUNT_SQL` inlines are the ones
/// it was derived from (a later migration that redefines one fails here
/// until the inline copy is re-derived).
#[tokio::test]
async fn library_topics_shape_pins() {
    let Ok(dsn) = std::env::var("P7_TEST_BASE_URL") else {
        assert!(std::env::var_os("REQUIRE_PG_SUITE").is_none(), "P7_TEST_BASE_URL required");
        return;
    };
    let opts = PgConnectOptions::from_str(&dsn).unwrap();
    assert!(matches!(opts.get_host(), "localhost" | "127.0.0.1" | "::1"));
    let admin = PgPoolOptions::new().max_connections(1).connect_with(opts.clone()).await.unwrap();
    let db = coralswarm_backend::test_db::TestDatabase::new(&opts, "p7_topics_fence");
    admin.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
    let owner = PgPoolOptions::new().max_connections(2).connect_with(opts.database(&db)).await.unwrap();
    sqlx::migrate!("./migrations_tenant").run(&owner).await.unwrap();
    use coralswarm_backend::store::library::{TOPICS_COUNT_SQL, TOPICS_PAGE_SQL};

    // The definitions the inline predicate restates.
    fn body<'a>(migration: &'a str, header: &str) -> &'a str {
        let rest = &migration[migration.find(header).unwrap_or_else(|| panic!("{header}"))..];
        let open = rest.find("$$").unwrap() + 2;
        let close = rest[open..].find("$$").unwrap();
        &rest[open..open + close]
    }
    for (function, migration, header) in [
        ("library_node_title", include_str!("../migrations_tenant/0157_reef_identity_corrections.sql"), "CREATE FUNCTION public.library_node_title(g UUID,n_id UUID)"),
        ("library_node_title_before_reef_identity", include_str!("../migrations_tenant/0137_library_read_boundary.sql"), "CREATE FUNCTION library_node_title(g UUID,n_id UUID)"),
        ("library_atom_permitted", include_str!("../migrations_tenant/0143_selected_evidence_lifecycle.sql"), "CREATE OR REPLACE FUNCTION library_atom_permitted(g UUID,a_id UUID)"),
        // 0204 (#1525) adds the reader's history floor. It reads only the
        // candidate atom row and a per-statement reader constant, so the
        // batched rewrites below still equal the per-row verdict.
        ("library_permitted_atoms", include_str!("../migrations_tenant/0204_history_floor_read_boundary.sql"), "CREATE OR REPLACE FUNCTION public.library_permitted_atoms(g UUID,atom_ids UUID[] DEFAULT NULL)"),
    ] {
        let live: Vec<String> = sqlx::query_scalar("SELECT prosrc FROM pg_proc WHERE proname=$1 AND pronamespace='public'::regnamespace")
            .bind(function).fetch_all(&owner).await.unwrap();
        assert_eq!(live, vec![body(migration, header).to_string()], "{function} changed: re-derive TOPICS_COUNT_SQL (store/library.rs) and the batched counts (store/verdict.rs)");
    }

    let f = batching_fixture(&owner).await;
    let graphs = vec![f.graph];
    let candidates: i64 = sqlx::query_scalar("SELECT count(*) FROM nodes n WHERE n.graph_id=ANY($1) AND n.entity_type IS NOT NULL AND (n.coral_count > 0 OR EXISTS (SELECT 1 FROM node_semantic_atoms sa WHERE sa.node_id=n.node_id AND sa.graph_id=n.graph_id))")
        .bind(&graphs).fetch_one(&owner).await.unwrap();
    assert!(candidates > 30, "{candidates}");

    // Superuser only: per-transaction call counts of a non-inlined function.
    let mut tx = owner.begin().await.unwrap();
    sqlx::query("SET LOCAL track_functions='all'").execute(&mut *tx).await.unwrap();
    async fn titled(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>) -> i64 {
        sqlx::query_scalar("SELECT COALESCE(pg_stat_get_xact_function_calls('public.library_node_title(uuid,uuid)'::regprocedure),0)")
            .fetch_one(&mut **tx).await.unwrap()
    }
    let mut seen = titled(&mut tx).await;
    macro_rules! title_calls {
        () => {{
            let now = titled(&mut tx).await;
            let delta = now - seen;
            seen = now;
            delta
        }};
    }
    for (limit, offset) in [(5i64, 0i64), (5, 5), (3, 20)] {
        let rows: Vec<TopicRow> = sqlx::query_as(TOPICS_PAGE_SQL).bind(&graphs).bind(None::<&str>).bind(None::<&str>).bind(limit).bind(offset).fetch_all(&mut *tx).await.unwrap();
        assert_eq!(rows.len() as i64, limit);
        assert_eq!(title_calls!(), limit + offset, "page {limit}+{offset}: titles only up to the page (the 30 newest candidates are all visible)");
    }
    let _: Vec<TopicRow> = sqlx::query_as(OLD_TOPICS_PAGE_SQL).bind(&graphs).bind(None::<&str>).bind(None::<&str>).bind(5i64).bind(0i64).fetch_all(&mut *tx).await.unwrap();
    assert_eq!(title_calls!(), candidates, "control: the old page titled every candidate");
    let _: i64 = sqlx::query_scalar(TOPICS_COUNT_SQL).bind(&graphs).bind(None::<&str>).bind(None::<&str>).fetch_one(&mut *tx).await.unwrap();
    assert_eq!(title_calls!(), 0, "the count computes no title");
    let _: i64 = sqlx::query_scalar(OLD_TOPICS_COUNT_SQL).bind(&graphs).bind(None::<&str>).bind(None::<&str>).fetch_one(&mut *tx).await.unwrap();
    assert_eq!(title_calls!(), candidates, "control: the old count titled every candidate");
    assert!(seen >= candidates);
    tx.rollback().await.unwrap();

    let plan: Vec<String> = sqlx::raw_sql(&format!("EXPLAIN (GENERIC_PLAN, COSTS OFF) {TOPICS_COUNT_SQL}")).fetch_all(&owner).await.unwrap()
        .iter().map(|row| row.get::<String, _>(0)).collect();
    let plan = plan.join("\n");
    assert!(plan.contains("hashed SubPlan"), "the permitted-set probe must stay hashed\n{plan}");
    assert!(!plan.contains("library_node_title") && !plan.contains("library_atom_permitted"), "{plan}");

    owner.close().await;
    admin.execute(format!("DROP DATABASE {db} WITH (FORCE)").as_str()).await.unwrap();
    admin.close().await;
}
