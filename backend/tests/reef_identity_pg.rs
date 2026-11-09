use std::{
    fs::OpenOptions,
    io::Write,
    str::FromStr,
    time::{Duration, Instant},
};

use coralswarm_backend::{
    retrieval::{node_query_tokens, RetrievalFilter, REEF_COVERAGE_FLOOR},
    services::reef_identity::patch_supported,
    store::{reef_lexical_query, reef_member_lexical_query, PostgresStore, Store, StoreError},
};
use serde_json::{json, Value};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    Acquire, Executor, PgPool, Postgres, Transaction,
};
use uuid::Uuid;

struct Fixture {
    owner: PgPool,
    app: PgPool,
    store: PostgresStore,
    org: bool,
    account: Uuid,
    actor: Uuid,
    other_account: Uuid,
    other_actor: Uuid,
    graph: Uuid,
    journal: String,
}

impl Fixture {
    async fn new(org: bool) -> Self {
        let opts = PgConnectOptions::from_str(
            &std::env::var("REEF_IDENTITY_TEST_BASE_URL")
                .expect("REEF_IDENTITY_TEST_BASE_URL is required; no silent skip"),
        )
        .unwrap();
        assert!(matches!(opts.get_host(), "localhost" | "127.0.0.1" | "::1"));
        assert_eq!(opts.get_database(), Some("postgres"));
        assert!(![52984, 61247, 61248, 61249].contains(&opts.get_port()));
        if let Ok(port) = std::env::var("REEF_IDENTITY_TEST_OWNED_PORT") {
            assert_eq!(opts.get_port(), port.parse::<u16>().unwrap());
        }
        let cluster = Uuid::parse_str(
            &std::env::var("REEF_IDENTITY_TEST_CLUSTER_ID")
                .expect("owned local cluster identity is required"),
        )
        .unwrap();
        let journal = std::env::var("REEF_IDENTITY_TEST_RESOURCE_LOG")
            .expect("append-only fixture resource/evidence journal is required");
        let nonce = Uuid::new_v4().simple().to_string();
        let db = format!(
            "reef_identity_{}_{nonce}",
            if org { "tenant" } else { "control" }
        );
        let role = format!("reef_identity_app_{nonce}");
        let owner_role = format!("reef_identity_owner_{nonce}");
        let password = Uuid::new_v4().simple().to_string();
        append(
            &journal,
            &json!({"event":"planned_retained_resources","cluster":cluster,
            "port":opts.get_port(),"database":db,"app_role":role,
            "migration_owner":if org { Some(&owner_role) } else { None }}),
        );
        let admin = connect(opts.clone()).await;
        let base = if org {
            "coralswarm_tenant_app"
        } else {
            "coralswarm_app"
        };
        let prerequisite: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1 AND NOT rolsuper AND NOT rolbypassrls AND NOT rolcreatedb AND NOT rolcreaterole AND NOT rolreplication)")
            .bind(base).fetch_one(&admin).await.unwrap();
        assert!(
            prerequisite,
            "prepare canonical ordinary app roles externally before running this suite"
        );
        if org {
            admin.execute(format!("CREATE ROLE {owner_role} LOGIN INHERIT NOSUPERUSER NOBYPASSRLS CREATEDB CREATEROLE NOREPLICATION PASSWORD '{password}'").as_str()).await.unwrap();
            for canonical in [
                "coralswarm_app",
                "coralswarm_tenant_app",
                "coralswarm_managed_clock",
            ] {
                let exists: bool = sqlx::query_scalar("SELECT to_regrole($1) IS NOT NULL")
                    .bind(canonical)
                    .fetch_one(&admin)
                    .await
                    .unwrap();
                if exists {
                    admin.execute(format!("GRANT {canonical} TO {owner_role} WITH ADMIN TRUE, INHERIT FALSE, SET TRUE").as_str()).await.unwrap();
                }
            }
            admin
                .execute(format!("CREATE DATABASE {db} OWNER {owner_role}").as_str())
                .await
                .unwrap();
        } else {
            admin
                .execute(format!("CREATE DATABASE {db}").as_str())
                .await
                .unwrap();
        }
        let extensions = connect(opts.clone().database(&db)).await;
        for extension in ["vector", "pg_trgm"] {
            extensions
                .execute(
                    format!("CREATE EXTENSION IF NOT EXISTS {extension} WITH SCHEMA public")
                        .as_str(),
                )
                .await
                .unwrap();
        }
        extensions.close().await;
        let owner = if org {
            connect(
                opts.clone()
                    .database(&db)
                    .username(&owner_role)
                    .password(&password),
            )
            .await
        } else {
            connect(opts.clone().database(&db)).await
        };
        if org {
            let attrs: (bool,bool,bool,bool,bool) = sqlx::query_as("SELECT rolsuper,rolbypassrls,rolreplication,rolcreatedb,rolcreaterole FROM pg_roles WHERE rolname=current_user").fetch_one(&owner).await.unwrap();
            assert_eq!(attrs, (false, false, false, true, true));
            sqlx::migrate!("./migrations_tenant")
                .run(&owner)
                .await
                .unwrap();
            owner
                .execute(
                    "INSERT INTO tenant_meta(id,org_id) VALUES(true,'org_reef_identity_fixture')",
                )
                .await
                .unwrap();
        } else {
            coralswarm_backend::CONTROL_MIGRATOR
                .run(&owner)
                .await
                .unwrap();
        }
        admin.execute(format!("CREATE ROLE {role} LOGIN INHERIT NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD '{password}' IN ROLE {base}").as_str()).await.unwrap();
        let app = connect(opts.database(&db).username(&role).password(&password)).await;
        let attrs: (String,String,bool,bool,bool,bool,bool,bool) = sqlx::query_as("SELECT current_user::text,session_user::text,rolsuper,rolbypassrls,rolcreatedb,rolcreaterole,rolreplication,rolinherit FROM pg_roles WHERE rolname=current_user").fetch_one(&app).await.unwrap();
        assert_eq!(
            attrs,
            (
                role.clone(),
                role.clone(),
                false,
                false,
                false,
                false,
                false,
                true
            )
        );
        let extras: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_roles r WHERE pg_has_role(current_user,r.oid,'MEMBER') AND r.rolname<>current_user AND r.rolname<>$1").bind(base).fetch_one(&app).await.unwrap();
        assert_eq!(extras, 0);
        let dangerous: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_roles r WHERE pg_has_role(current_user,r.oid,'MEMBER') AND (r.rolsuper OR r.rolbypassrls OR r.rolcreatedb OR r.rolcreaterole OR r.rolreplication OR EXISTS(SELECT 1 FROM pg_class c WHERE c.relowner=r.oid AND c.relnamespace='public'::regnamespace))").fetch_one(&app).await.unwrap();
        assert_eq!(dangerous, 0);
        for privilege in ["SELECT", "INSERT", "UPDATE", "DELETE"] {
            let mismatch: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_class c WHERE c.relnamespace='public'::regnamespace AND c.relkind IN('r','p','v') AND has_table_privilege($1,c.oid,$2) IS DISTINCT FROM has_table_privilege(current_user,c.oid,$2)").bind(base).bind(privilege).fetch_one(&app).await.unwrap();
            assert_eq!(mismatch, 0, "{privilege} ACL parity");
        }
        let functions: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_proc p WHERE p.pronamespace='public'::regnamespace AND has_function_privilege($1,p.oid,'EXECUTE') IS DISTINCT FROM has_function_privilege(current_user,p.oid,'EXECUTE')").bind(base).fetch_one(&app).await.unwrap();
        assert_eq!(functions, 0);
        let policies: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_policy p WHERE p.polrelid IN(SELECT oid FROM pg_class WHERE relnamespace='public'::regnamespace) AND EXISTS(SELECT 1 FROM unnest(p.polroles) r WHERE CASE WHEN r=0 THEN true ELSE pg_has_role(current_user,r,'MEMBER') END) IS DISTINCT FROM EXISTS(SELECT 1 FROM unnest(p.polroles) r WHERE CASE WHEN r=0 THEN true ELSE pg_has_role($1::text,r,'MEMBER') END)").bind(base).fetch_one(&app).await.unwrap();
        assert_eq!(policies, 0);
        let exposed: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_proc p WHERE p.pronamespace='public'::regnamespace AND p.proname LIKE 'reef_identity_%' AND p.proname NOT IN('reef_identity_atom_fingerprint','reef_identity_support_valid','reef_identity_text_vectors','reef_identity_contains_label','reef_identity_sanitize_json','reef_identity_sanitize_body') AND has_function_privilege(current_user,p.oid,'EXECUTE')").fetch_one(&app).await.unwrap();
        assert_eq!(exposed, 0);
        let (account, actor, other_account, other_actor) = if org {
            (
                Uuid::new_v4(),
                Uuid::new_v4(),
                Uuid::new_v4(),
                Uuid::new_v4(),
            )
        } else {
            let mut identities = Vec::new();
            for _ in 0..2 {
                let actor: Uuid =
                    sqlx::query_scalar("INSERT INTO users(email) VALUES($1) RETURNING id")
                        .bind(format!("reef-{}@test.local", Uuid::new_v4()))
                        .fetch_one(&owner)
                        .await
                        .unwrap();
                let account: Uuid = sqlx::query_scalar(
                    "INSERT INTO accounts(owner_user_id) VALUES($1) RETURNING id",
                )
                .bind(actor)
                .fetch_one(&owner)
                .await
                .unwrap();
                sqlx::query(
                    "INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'owner')",
                )
                .bind(account)
                .bind(actor)
                .execute(&owner)
                .await
                .unwrap();
                identities.push((account, actor));
            }
            (
                identities[0].0,
                identities[0].1,
                identities[1].0,
                identities[1].1,
            )
        };
        let graph: Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'Reef identity fixture','member',$2) RETURNING id").bind(account).bind(actor).fetch_one(&owner).await.unwrap();
        append(
            &journal,
            &json!({"event":"normal_role_audit","database":db,"role":role,"canonical":base,"owner":owner_role,"tenant_migrator_nonsuper":org,"extra_memberships":extras,"privileged_memberships":dangerous,"function_acl_mismatches":functions,"policy_mismatches":policies,"cleanup_execute_grants":exposed}),
        );
        admin.close().await;
        Self {
            store: PostgresStore::new(app.clone()),
            owner,
            app,
            org,
            account,
            actor,
            other_account,
            other_actor,
            graph,
            journal,
        }
    }
    async fn tx_as(&self, account: Uuid, actor: Uuid) -> Transaction<'_, Postgres> {
        let mut tx = self.app.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.account_id',$1,true),set_config('app.user_id',$2,true),set_config('app.tenant_actor','member',true)").bind(account.to_string()).bind(actor.to_string()).execute(&mut *tx).await.unwrap();
        tx
    }
    async fn tx(&self) -> Transaction<'_, Postgres> {
        self.tx_as(self.account, self.actor).await
    }
    async fn atom(&self, graph: Uuid, body: &str) -> Uuid {
        sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'note',$2,'synthetic-reef-fixture',$3,now()) RETURNING atom_id").bind(graph).bind(format!("reef:{}",Uuid::new_v4())).bind(body).fetch_one(&self.owner).await.unwrap()
    }
    async fn node(&self, graph: Uuid, title: &str, atoms: &[Uuid]) -> Uuid {
        let id=sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title,body) VALUES($1,'entity',$2,'{}') RETURNING node_id").bind(graph).bind(title).fetch_one(&self.owner).await.unwrap();
        sqlx::query("INSERT INTO node_atoms(node_id,atom_id) SELECT $1,unnest($2::uuid[])")
            .bind(id)
            .bind(atoms)
            .execute(&self.owner)
            .await
            .unwrap();
        id
    }
    async fn rename(&self, node: Uuid, title: &str, support: Uuid) -> Uuid {
        let mut tx = self.tx().await;
        patch_supported(
            &self.store,
            &mut tx,
            self.account,
            self.actor,
            node,
            title,
            None,
            None,
            support,
        )
        .await
        .unwrap();
        let event=sqlx::query_scalar("SELECT correction_id FROM reef_identity_corrections WHERE node_id=$1 AND state='active'").bind(node).fetch_one(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
        event
    }
    async fn title(&self, node: Uuid) -> Option<String> {
        let mut tx = self.tx().await;
        let title = sqlx::query_scalar("SELECT library_node_title($1,$2)")
            .bind(self.graph)
            .bind(node)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        title
    }
    async fn assert_hit(&self, query: &str, node: Uuid, atom: Uuid) {
        let mut tx = self.tx().await;
        let hits = self
            .store
            .matched_reef_ids_scoped(&mut tx, self.graph, query, 20, &RetrievalFilter::default())
            .await
            .unwrap();
        assert!(
            hits.iter().any(|(id, _)| *id == node),
            "missing reef for {query}: {hits:?}"
        );
        let atoms = self
            .store
            .keyword_search_node_atoms_scoped(
                &mut tx,
                self.graph,
                query,
                20,
                &RetrievalFilter::default(),
            )
            .await
            .unwrap();
        assert!(
            atoms.iter().any(|(id, _)| *id == atom),
            "missing admitted member for {query}"
        );
        tx.commit().await.unwrap();
    }
    async fn assert_absent(&self, query: &str, node: Uuid) {
        let mut tx = self.tx().await;
        let hits = self
            .store
            .matched_reef_ids_scoped(&mut tx, self.graph, query, 20, &RetrievalFilter::default())
            .await
            .unwrap();
        assert!(
            !hits.iter().any(|(id, _)| *id == node),
            "unauthorized reef for {query}"
        );
        tx.commit().await.unwrap();
    }
    async fn source(&self, atom: Uuid, access: &str) -> (Uuid, Uuid) {
        let source = format!("bound:{}", Uuid::new_v4());
        let item:Uuid=sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,access_state) VALUES($1,'note','fixture:reef-identity',$2,$3) RETURNING source_item_id").bind(self.graph).bind(&source).bind(access).fetch_one(&self.owner).await.unwrap();
        let version:Uuid=sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,raw_inline,extraction_version) VALUES($1,$2,$3,'note','fixture:reef-identity','Synthetic retained evidence','source-atoms-v1') RETURNING version_id").bind(self.graph).bind(item).bind(source).fetch_one(&self.owner).await.unwrap();
        sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
            .bind(item)
            .bind(version)
            .execute(&self.owner)
            .await
            .unwrap();
        sqlx::query("INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) VALUES($1,$2,$3)").bind(self.graph).bind(version).bind(atom).execute(&self.owner).await.unwrap();
        (item, version)
    }
    async fn close(self) {
        self.app.close().await;
        self.owner.close().await;
    }
}

fn append(path: &str, value: &Value) {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    writeln!(file, "{value}").unwrap();
    file.sync_all().unwrap();
}
async fn connect(opts: PgConnectOptions) -> PgPool {
    PgPoolOptions::new()
        .max_connections(2)
        .after_connect(|conn, _| {
            Box::pin(async move {
                conn.execute("SET statement_timeout='10s'").await?;
                Ok(())
            })
        })
        .connect_with(opts)
        .await
        .unwrap()
}

async fn account_scope_actor_contract(f: &Fixture) {
    let support = f.atom(f.graph, "Anonymous scope boundary fixture record.").await;
    let node = f.node(f.graph, "Before actor scope fixture", &[support]).await;
    let before: Value = sqlx::query_scalar("SELECT jsonb_build_array(to_jsonb(n),(SELECT jsonb_agg(to_jsonb(c) ORDER BY correction_id) FROM reef_identity_corrections c WHERE c.node_id=n.node_id),(SELECT jsonb_agg(to_jsonb(o) ORDER BY seq) FROM op_log o WHERE o.entity='node' AND o.entity_id=n.node_id::text)) FROM nodes n WHERE n.node_id=$1")
        .bind(node).fetch_one(&f.owner).await.unwrap();
    // This is the real CONTROL route/eval opener, not f.tx()'s actor setup.
    let mut account_tx = coralswarm_backend::tenancy::scope::begin_account_scoped(&f.app, f.account)
        .await.unwrap();
    let (tenant_only, actor): (bool, Option<String>) = sqlx::query_as(
        "SELECT to_regprocedure('public.tenant_rls_actor_user_id()') IS NOT NULL,current_setting('app.user_id',true)",
    ).fetch_one(&mut *account_tx).await.unwrap();
    assert_eq!(tenant_only, f.org);
    assert!(actor.as_deref().unwrap_or("").is_empty());
    let result = patch_supported(
        &f.store, &mut account_tx, f.account, f.actor, node,
        "Scoped Actor Identity", None, None, support,
    ).await;
    if f.org {
        match result {
            Err(StoreError::Conflict(message)) => assert_eq!(message, "identity actor scope unavailable"),
            other => panic!("Tenant account-only scope must reject missing actor: {other:?}"),
        }
        account_tx.commit().await.unwrap();
        let after: Value = sqlx::query_scalar("SELECT jsonb_build_array(to_jsonb(n),(SELECT jsonb_agg(to_jsonb(c) ORDER BY correction_id) FROM reef_identity_corrections c WHERE c.node_id=n.node_id),(SELECT jsonb_agg(to_jsonb(o) ORDER BY seq) FROM op_log o WHERE o.entity='node' AND o.entity_id=n.node_id::text)) FROM nodes n WHERE n.node_id=$1")
            .bind(node).fetch_one(&f.owner).await.unwrap();
        assert_eq!(before, after, "caught missing-actor error plus outer COMMIT preserves node/event/oplog");
        let mut member_tx = coralswarm_backend::tenancy::scope::begin_tenant_scoped(&f.app, f.account, f.actor)
            .await.unwrap();
        patch_supported(
            &f.store, &mut member_tx, f.account, f.actor, node,
            "Scoped Actor Identity", None, None, support,
        ).await.unwrap();
        member_tx.commit().await.unwrap();
    } else {
        result.unwrap();
        let actor: String = sqlx::query_scalar("SELECT current_setting('app.user_id')")
            .fetch_one(&mut *account_tx).await.unwrap();
        assert_eq!(actor, f.actor.to_string());
        account_tx.commit().await.unwrap();
    }
    assert_eq!(f.title(node).await.as_deref(), Some("Scoped Actor Identity"));
    let recorded: Uuid = sqlx::query_scalar("SELECT recorded_by FROM reef_identity_corrections WHERE node_id=$1 AND state='active'")
        .bind(node).fetch_one(&f.owner).await.unwrap();
    assert_eq!(recorded, f.actor);
    append(&f.journal, &json!({"event":"actual_scope_actor_contract_passed","tenant":f.org,"tenant_only_actor_function_present":tenant_only,"control_account_only_authenticated_actor":!f.org,"tenant_missing_actor_outer_commit_unchanged":f.org}));
}

async fn security_and_lifecycle(org: bool) {
    let f = Fixture::new(org).await;
    account_scope_actor_contract(&f).await;
    let support = f
        .atom(
            f.graph,
            "Anonymous engineer discussed lighthouse deployment reliability and rollback planning.",
        )
        .await;
    let node = f
        .node(f.graph, "Uncurated interpretation", &[support])
        .await;
    assert_eq!(f.title(node).await, None);
    f.assert_absent("Uncurated interpretation", node).await;
    let first = f.rename(node, "Anchor Meridian", support).await;
    assert_eq!(f.title(node).await.as_deref(), Some("Anchor Meridian"));
    for query in [
        "Anchor Meridian",
        "What did Anchor Meridian discuss about deployment reliability and rollback planning?",
        "lighthouse deployment",
        "rollback planning",
    ] {
        f.assert_hit(query, node, support).await;
    }
    let original: String = sqlx::query_scalar("SELECT content_inline FROM atoms WHERE atom_id=$1")
        .bind(support)
        .fetch_one(&f.owner)
        .await
        .unwrap();
    assert!(!original.contains("Anchor Meridian"));
    let before:Value=sqlx::query_scalar("SELECT jsonb_build_array(to_jsonb(n),(SELECT jsonb_agg(to_jsonb(c) ORDER BY correction_id) FROM reef_identity_corrections c WHERE c.node_id=n.node_id),(SELECT jsonb_agg(to_jsonb(o) ORDER BY seq) FROM op_log o WHERE o.entity='node' AND o.entity_id=n.node_id::text)) FROM nodes n WHERE n.node_id=$1").bind(node).fetch_one(&f.owner).await.unwrap();
    let mut tx = f.tx().await;
    let stale = patch_supported(
        &f.store,
        &mut tx,
        f.account,
        f.actor,
        node,
        "Stale Mutation",
        None,
        Some(1),
        support,
    )
    .await;
    assert!(stale.is_err());
    tx.commit().await.unwrap();
    let after:Value=sqlx::query_scalar("SELECT jsonb_build_array(to_jsonb(n),(SELECT jsonb_agg(to_jsonb(c) ORDER BY correction_id) FROM reef_identity_corrections c WHERE c.node_id=n.node_id),(SELECT jsonb_agg(to_jsonb(o) ORDER BY seq) FROM op_log o WHERE o.entity='node' AND o.entity_id=n.node_id::text)) FROM nodes n WHERE n.node_id=$1").bind(node).fetch_one(&f.owner).await.unwrap();
    assert_eq!(
        before, after,
        "handled service error must preserve the outer transaction"
    );
    let mut tx = f.tx().await;
    f.store
        .patch_node_scoped(&mut tx, node, None, Some("thread"), None)
        .await
        .unwrap()
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(f.title(node).await.as_deref(), Some("Anchor Meridian"));
    let raw = f.node(f.graph, "Raw Invented Name", &[support]).await;
    let mut tx = f.tx().await;
    f.store
        .patch_node_scoped(&mut tx, raw, None, Some("thread"), None)
        .await
        .unwrap()
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        f.title(raw).await,
        None,
        "type-only edits do not mint authority"
    );
    let (revision, fingerprint): (i64, String) = sqlx::query_as(
        "SELECT content_revision,reef_identity_atom_fingerprint(atoms) FROM atoms WHERE atom_id=$1",
    )
    .bind(support)
    .fetch_one(&f.owner)
    .await
    .unwrap();
    for (actor_setting, recorded_by, system) in [
        (String::new(), f.actor, false),
        (f.actor.to_string(), f.other_actor, false),
        (f.actor.to_string(), f.actor, true),
    ] {
        let mut tx = f.tx().await;
        sqlx::query(
            "SELECT set_config('app.user_id',$1,true),set_config('app.tenant_actor',$2,true)",
        )
        .bind(actor_setting)
        .bind(if system { "system" } else { "member" })
        .execute(&mut *tx)
        .await
        .unwrap();
        let forged=sqlx::query("INSERT INTO public.reef_identity_corrections(graph_id,node_id,label,recorded_by,support_atom_id,support_kind,content_revision,input_sha256) VALUES($1,$2,'Raw Invented Name',$3,$4,'legacy',$5,$6)")
            .bind(f.graph).bind(raw).bind(recorded_by).bind(support).bind(revision).bind(&fingerprint).execute(&mut *tx).await;
        assert!(
            forged.is_err(),
            "blank, forged or system actor cannot mint identity"
        );
        tx.rollback().await.unwrap();
    }
    let foreign:Uuid=sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'Foreign fixture','member',$2) RETURNING id").bind(f.other_account).bind(f.other_actor).fetch_one(&f.owner).await.unwrap();
    let foreign_atom = f.atom(foreign, "foreign private content").await;
    let foreign_node = f.node(foreign, "Anchor Meridian", &[foreign_atom]).await;
    for (candidate, member, account, actor) in [
        (foreign_node, foreign_atom, f.account, f.actor),
        (node, foreign_atom, f.account, f.actor),
        (node, support, f.other_account, f.other_actor),
    ] {
        let mut tx = f.tx().await;
        assert!(patch_supported(
            &f.store,
            &mut tx,
            account,
            actor,
            candidate,
            "Invalid Authority",
            None,
            None,
            member
        )
        .await
        .is_err());
        tx.commit().await.unwrap();
    }
    let same = f
        .node(f.graph, "Anchor Meridian duplicate", &[support])
        .await;
    assert_eq!(
        f.title(same).await,
        None,
        "authority stays bound to exact node"
    );
    let restricted = f
        .atom(f.graph, "forbiddensecret lexical contamination")
        .await;
    f.source(restricted, "restricted").await;
    let unknown = f.atom(f.graph, "unknownsecret lexical contamination").await;
    f.source(unknown, "unknown").await;
    sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2),($1,$3)")
        .bind(node)
        .bind(restricted)
        .bind(unknown)
        .execute(&f.owner)
        .await
        .unwrap();
    for denied in [restricted, unknown] {
        let mut tx = f.tx().await;
        assert!(patch_supported(
            &f.store,
            &mut tx,
            f.account,
            f.actor,
            node,
            "Denied Support",
            None,
            None,
            denied
        )
        .await
        .is_err());
        tx.commit().await.unwrap();
    }
    for query in ["forbiddensecret", "unknownsecret"] {
        f.assert_absent(query, node).await;
    }
    let mut tx = f.tx().await;
    let atoms = f
        .store
        .keyword_search_node_atoms_scoped(
            &mut tx,
            f.graph,
            "Anchor Meridian",
            20,
            &RetrievalFilter::default(),
        )
        .await
        .unwrap();
    assert!(atoms.iter().any(|(id, _)| *id == support));
    assert!(!atoms
        .iter()
        .any(|(id, _)| *id == restricted || *id == unknown));
    tx.commit().await.unwrap();
    let mixed = f
        .atom(f.graph, "mixedsecret should never be admitted")
        .await;
    f.source(mixed, "permitted").await;
    f.source(mixed, "restricted").await;
    let mixed_node = f.node(f.graph, "Mixed Raw", &[mixed]).await;
    let mut tx = f.tx().await;
    assert!(patch_supported(
        &f.store,
        &mut tx,
        f.account,
        f.actor,
        mixed_node,
        "Mixed Forbidden",
        None,
        None,
        mixed
    )
    .await
    .is_err());
    tx.commit().await.unwrap();
    f.assert_absent("mixedsecret", mixed_node).await;
    for title in ["Different raw label", "Anchor Meridian"] {
        let mut tx = f.tx().await;
        f.store
            .patch_node_scoped(&mut tx, node, Some(title), None, None)
            .await
            .unwrap()
            .unwrap();
        tx.commit().await.unwrap();
    }
    assert_eq!(
        f.title(node).await,
        None,
        "raw A to B to A must not resurrect identity"
    );
    let second = f.rename(node, "Anchor Meridian", support).await;
    assert_ne!(first, second);
    let mut tx = f.tx().await;
    sqlx::query("DELETE FROM node_atoms WHERE node_id=$1 AND atom_id=$2")
        .bind(node)
        .bind(support)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)")
        .bind(node)
        .bind(support)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(f.title(node).await, None);
    f.rename(node, "Anchor Meridian", support).await;
    for content in [
        "different replacement",
        "Anonymous engineer discussed lighthouse deployment reliability and rollback planning.",
    ] {
        let mut tx = f.tx().await;
        sqlx::query("UPDATE atoms SET content_inline=$2 WHERE atom_id=$1")
            .bind(support)
            .bind(content)
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    assert_eq!(
        f.title(node).await,
        None,
        "X to Y to X bytes must not resurrect"
    );
    f.rename(node, "Anchor Meridian", support).await;
    for participants in [vec!["changed participant".to_string()], Vec::new()] {
        let mut tx = f.tx().await;
        sqlx::query("UPDATE atoms SET participants=$2 WHERE atom_id=$1")
            .bind(support)
            .bind(participants)
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    assert_eq!(
        f.title(node).await,
        None,
        "metadata round trip must not resurrect"
    );
    f.rename(node, "Anchor Meridian", support).await;
    let (item, v1) = f.source(support, "permitted").await;
    assert_eq!(
        f.title(node).await,
        None,
        "legacy becoming bound invalidates authority"
    );
    f.rename(node, "Anchor Meridian", support).await;
    for participants in [vec!["bound metadata replacement".to_string()], Vec::new()] {
        let mut tx = f.tx().await;
        sqlx::query("UPDATE atoms SET participants=$2 WHERE atom_id=$1")
            .bind(support)
            .bind(participants)
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    assert_eq!(
        f.title(node).await,
        None,
        "bound chunk metadata round trip cannot resurrect authority"
    );
    f.rename(node, "Anchor Meridian", support).await;
    let v2:Uuid=sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,raw_inline,extraction_version) SELECT graph_id,source_item_id,source_id,source_type,provider_namespace,'second immutable version','source-atoms-v1' FROM source_items WHERE source_item_id=$1 RETURNING version_id").bind(item).fetch_one(&f.owner).await.unwrap();
    for version in [v2, v1] {
        sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
            .bind(item)
            .bind(version)
            .execute(&f.owner)
            .await
            .unwrap();
    }
    assert_eq!(
        f.title(node).await,
        None,
        "version pointer round trip must not resurrect"
    );
    f.rename(node, "Anchor Meridian", support).await;
    let mut tx = f.tx().await;
    let rebind=sqlx::query("UPDATE reef_identity_corrections SET support_kind='legacy',source_item_id=NULL,source_version_id=NULL WHERE node_id=$1 AND state='active'").bind(node).execute(&mut *tx).await;
    assert!(rebind.is_err());
    tx.rollback().await.unwrap();
    sqlx::query("DELETE FROM source_version_chunks WHERE source_version_id=$1 AND atom_id=$2")
        .bind(v1)
        .bind(support)
        .execute(&f.owner)
        .await
        .unwrap();
    assert_eq!(
        f.title(node).await,
        None,
        "mapping removal cannot downgrade bound support"
    );
    sqlx::query(
        "INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) VALUES($1,$2,$3)",
    )
    .bind(f.graph)
    .bind(v1)
    .bind(support)
    .execute(&f.owner)
    .await
    .unwrap();
    assert_eq!(
        f.title(node).await,
        None,
        "mapping restoration cannot restore old event"
    );
    f.rename(node, "Anchor Meridian", support).await;
    semantic_context(&f).await;
    merge_and_shadow(&f).await;
    cross_account_and_copy(&f).await;
    retained_admission_merge_denials(&f).await;
    source_purge(&f).await;
    purge_history(&f, node, support).await;
    membership_and_departure(&f).await;
    if f.org {
        asymmetric_tenant_history(&f).await;
    }
    private_graph_erasure(&f).await;
    independent_history_preservation(&f).await;
    sanitizer_value_contract(&f).await;
    merge_cached_history_preservation(&f).await;
    purge_queue_authority_and_delivery(&f).await;
    append(
        &f.journal,
        &json!({"event":"security_lifecycle_passed","plane":if org{"tenant"}else{"consumer"}}),
    );
    f.close().await;
}

async fn purge_history(f: &Fixture, node: Uuid, support: Uuid) {
    let surviving = f
        .atom(f.graph, "Independent unrelated retained member.")
        .await;
    sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)")
        .bind(node)
        .bind(surviving)
        .execute(&f.owner)
        .await
        .unwrap();
    f.rename(node, "Historic Confidential Identity", support)
        .await;
    f.rename(node, "Current Confidential Identity", support)
        .await;
    sqlx::query("UPDATE nodes SET body=jsonb_build_object('digest','Historic Confidential Identity and Current Confidential Identity','aliases',jsonb_build_array('Historic Confidential Identity')) WHERE node_id=$1").bind(node).execute(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO node_merges(graph_id,keep_id,dup_id,dup_snapshot) SELECT graph_id,$2,node_id,to_jsonb(nodes) FROM nodes WHERE node_id=$1").bind(node).bind(Uuid::new_v4()).execute(&f.owner).await.unwrap();
    let seq_before: i64 =
        sqlx::query_scalar("SELECT COALESCE(max(seq),0) FROM op_log WHERE graph_id=$1")
            .bind(f.graph)
            .fetch_one(&f.owner)
            .await
            .unwrap();
    let mut tx = f.tx().await;
    sqlx::query("DELETE FROM atoms WHERE atom_id=$1")
        .bind(support)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let leaks:i64=sqlx::query_scalar("SELECT (SELECT count(*) FROM reef_identity_corrections WHERE node_id=$1 AND(label IS NOT NULL OR recorded_by IS NOT NULL))+(SELECT count(*) FROM nodes WHERE node_id=$1 AND(to_jsonb(nodes)::text LIKE '%Confidential Identity%'))+(SELECT count(*) FROM op_log WHERE graph_id=$2 AND entity='node' AND entity_id=$1::text AND payload::text LIKE '%Confidential Identity%')+(SELECT count(*) FROM node_merges WHERE graph_id=$2 AND dup_id=$1 AND dup_snapshot::text LIKE '%Confidential Identity%')").bind(node).bind(f.graph).fetch_one(&f.owner).await.unwrap();
    assert_eq!(
        leaks, 0,
        "purge must erase historical labels/actors and cached snapshots"
    );
    let new_seq: i64 = sqlx::query_scalar(
        "SELECT max(seq) FROM op_log WHERE graph_id=$1 AND entity='node' AND entity_id=$2::text",
    )
    .bind(f.graph)
    .bind(node)
    .fetch_one(&f.owner)
    .await
    .unwrap();
    assert!(
        new_seq > seq_before,
        "sanitization must emit fresh sync sequence"
    );
    let remaining: i64 =
        sqlx::query_scalar("SELECT count(*) FROM node_atoms WHERE node_id=$1 AND atom_id=$2")
            .bind(node)
            .bind(surviving)
            .fetch_one(&f.owner)
            .await
            .unwrap();
    assert_eq!(remaining, 1);
    assert_eq!(f.title(node).await, None);
    let literal = f
        .atom(
            f.graph,
            "Independent Literal Identity is independently recorded.",
        )
        .await;
    let selected = f
        .atom(f.graph, "anonymous support selected by editor")
        .await;
    let independent = f
        .node(f.graph, "before literal support", &[literal, selected])
        .await;
    f.rename(independent, "Independent Literal Identity", selected)
        .await;
    let mut tx = f.tx().await;
    sqlx::query("DELETE FROM atoms WHERE atom_id=$1")
        .bind(selected)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        f.title(independent).await.as_deref(),
        Some("Independent Literal Identity")
    );
    f.assert_hit("Independent Literal Identity", independent, literal)
        .await;
}

async fn semantic_context(f: &Fixture) {
    let support = f
        .atom(f.graph, "anonymous independently sufficient support")
        .await;
    let node = f
        .node(f.graph, "semantic initial raw label", &[support])
        .await;
    f.rename(node, "Semantic Context Identity", support).await;
    for (access, statement, ordinal) in [
        ("permitted", "semanticquartz maintenance scheduling", 0_i32),
        (
            "restricted",
            "semanticrestricted confidential context",
            1_i32,
        ),
        ("unknown", "semanticunknown unverified context", 2_i32),
    ] {
        let atom = f
            .atom(f.graph, "source-backed semantic fixture input")
            .await;
        let (_, version) = f.source(atom, access).await;
        let semantic:Uuid=sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model,entities) VALUES($1,$2,$3,'fact',$4,'source-atoms-v1','deterministic-fixture','[]') RETURNING semantic_atom_id").bind(f.graph).bind(version).bind(ordinal).bind(statement).fetch_one(&f.owner).await.unwrap();
        sqlx::query(
            "INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)",
        )
        .bind(f.graph)
        .bind(node)
        .bind(semantic)
        .execute(&f.owner)
        .await
        .unwrap();
    }
    f.assert_hit("semanticquartz maintenance", node, support)
        .await;
    for denied in ["semanticrestricted", "semanticunknown"] {
        f.assert_absent(denied, node).await;
    }
}

async fn asymmetric_tenant_history(f: &Fixture) {
    windowed_merge_guard(f).await;
    assert!(
        f.org,
        "per-atom capture windows are an existing Tenant-only policy"
    );
    let graph:Uuid=sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind) VALUES($1,'Asymmetric capture-window fixture','team') RETURNING id").bind(f.account).fetch_one(&f.owner).await.unwrap();
    let team:Uuid=sqlx::query_scalar("INSERT INTO teams(account_id,graph_id,name,created_by) VALUES($1,$2,'Asymmetric history fixture',$3) RETURNING id").bind(f.account).bind(graph).bind(f.actor).fetch_one(&f.owner).await.unwrap();
    for actor in [f.actor, f.other_actor] {
        sqlx::query("INSERT INTO team_members(team_id,user_id,role) VALUES($1,$2,'member')")
            .bind(team)
            .bind(actor)
            .execute(&f.owner)
            .await
            .unwrap();
    }
    sqlx::query("INSERT INTO team_seat_grants(account_id,team_id,user_id,source,history_from) VALUES($1,$2,$3,'fixture:windowed','2026-01-01T00:00:00Z'),($1,$2,$4,'fixture:full-history',NULL)")
        .bind(f.account).bind(team).bind(f.actor).bind(f.other_actor).execute(&f.owner).await.unwrap();
    let selected = f
        .atom(graph, "anonymous recent selected interpretation support")
        .await;
    let retained=f.atom(graph,"Window Retained Identity is independently attested. Windowreaderrare preserves older context.").await;
    sqlx::query("UPDATE atoms SET timestamp=CASE WHEN atom_id=$1 THEN '2026-02-01T00:00:00Z'::timestamptz ELSE '2025-12-01T00:00:00Z'::timestamptz END WHERE atom_id IN($1,$2)").bind(selected).bind(retained).execute(&f.owner).await.unwrap();
    let node = f
        .node(graph, "before asymmetric identity", &[selected, retained])
        .await;
    let mut tx = f.tx().await;
    let visible: Vec<Uuid> =
        sqlx::query_scalar("SELECT atom_id FROM atoms WHERE graph_id=$1 ORDER BY atom_id")
            .bind(graph)
            .fetch_all(&mut *tx)
            .await
            .unwrap();
    assert_eq!(
        visible,
        vec![selected],
        "deleting actor must genuinely be unable to read independent older B support"
    );
    patch_supported(
        &f.store,
        &mut tx,
        f.account,
        f.actor,
        node,
        "Window Retained Identity",
        None,
        None,
        selected,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let body = json!({"summary":"Independent window history remains","human_review_status":"confirmed","metadata":{"other_reader_context":"preserved"}});
    sqlx::query("UPDATE nodes SET body=$2 WHERE node_id=$1")
        .bind(node)
        .bind(&body)
        .execute(&f.owner)
        .await
        .unwrap();
    let mut tx = f.tx().await;
    f.store
        .patch_node_scoped(&mut tx, node, None, Some("thread"), None)
        .await
        .unwrap()
        .unwrap();
    tx.commit().await.unwrap();
    sqlx::query("INSERT INTO node_merges(graph_id,keep_id,dup_id,dup_snapshot) SELECT graph_id,$2,node_id,to_jsonb(nodes) FROM nodes WHERE node_id=$1").bind(node).bind(Uuid::new_v4()).execute(&f.owner).await.unwrap();
    let mut deleting = f.tx().await;
    sqlx::query("DELETE FROM atoms WHERE atom_id=$1")
        .bind(selected)
        .execute(&mut *deleting)
        .await
        .unwrap();
    deleting.commit().await.unwrap();
    let stored: (String, Value) = sqlx::query_as("SELECT title,body FROM nodes WHERE node_id=$1")
        .bind(node)
        .fetch_one(&f.owner)
        .await
        .unwrap();
    assert_eq!(stored,("Window Retained Identity".into(),body.clone()),"privileged cleanup must preserve independent B globally despite deleting reader's narrower window");
    let snapshot: Value =
        sqlx::query_scalar("SELECT dup_snapshot FROM node_merges WHERE dup_id=$1")
            .bind(node)
            .fetch_one(&f.owner)
            .await
            .unwrap();
    assert_eq!(snapshot["title"], "Window Retained Identity");
    assert_eq!(decode_body(&snapshot["body"]), body);
    let histories:Vec<Value>=sqlx::query_scalar("SELECT payload FROM op_log WHERE graph_id=$1 AND entity='node' AND entity_id=$2::text ORDER BY seq").bind(graph).bind(node).fetch_all(&f.owner).await.unwrap();
    assert!(histories
        .iter()
        .any(|entry| entry["title"] == "Window Retained Identity"
            && decode_body(&entry["body"])["summary"] == "Independent window history remains"));
    for (actor, allowed) in [(f.actor, false), (f.other_actor, true)] {
        let mut tx = f.tx_as(f.account, actor).await;
        let safe: Option<String> = sqlx::query_scalar("SELECT public.library_node_title($1,$2)")
            .bind(graph)
            .bind(node)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        assert_eq!(
            safe.as_deref(),
            if allowed {
                Some("Window Retained Identity")
            } else {
                None
            }
        );
        let reefs = f
            .store
            .matched_reef_ids_scoped(
                &mut tx,
                graph,
                "windowreaderrare",
                20,
                &RetrievalFilter::default(),
            )
            .await
            .unwrap();
        assert_eq!(reefs.iter().any(|(id, _)| *id == node), allowed);
        let atoms = f
            .store
            .keyword_search_node_atoms_scoped(
                &mut tx,
                graph,
                "windowreaderrare",
                20,
                &RetrievalFilter::default(),
            )
            .await
            .unwrap();
        assert_eq!(atoms.iter().any(|(id, _)| *id == retained), allowed);
        tx.commit().await.unwrap();
    }
    append(
        &f.journal,
        &json!({"event":"asymmetric_tenant_capture_window_history_passed","deleting_actor_independent_support_visible":false,"other_reader_independent_support_visible":true,"consumer_equivalent":"not claimed: CONTROL0184 adds schema parity only; per-atom capture-window RLS is Tenant0064"}),
    );
}

async fn merge_state_snapshot(f: &Fixture, graph: Uuid) -> Value {
    sqlx::query_scalar("SELECT jsonb_build_object('nodes',(SELECT jsonb_agg(to_jsonb(n) ORDER BY n.node_id) FROM nodes n WHERE n.graph_id=$1),'links',(SELECT jsonb_agg(to_jsonb(l) ORDER BY l.node_id,l.atom_id) FROM node_atoms l JOIN nodes n ON n.node_id=l.node_id WHERE n.graph_id=$1),'semantic_links',(SELECT jsonb_agg(to_jsonb(l) ORDER BY l.node_id,l.semantic_atom_id) FROM node_semantic_atoms l WHERE l.graph_id=$1),'corrections',(SELECT jsonb_agg(to_jsonb(c) ORDER BY c.correction_id) FROM reef_identity_corrections c WHERE c.graph_id=$1),'op_log',(SELECT jsonb_agg(to_jsonb(o) ORDER BY o.seq) FROM op_log o WHERE o.graph_id=$1),'node_merges',(SELECT jsonb_agg(to_jsonb(m) ORDER BY m.merge_id) FROM node_merges m WHERE m.graph_id=$1))").bind(graph).fetch_one(&f.owner).await.unwrap()
}

async fn windowed_merge_guard(f: &Fixture) {
    // Tenant0009 node_atoms and Tenant0126 node_semantic_atoms expose links by
    // visible node/graph; Tenant0064 applies capture windows to atoms separately.
    // The real policies therefore let the guard enumerate excluded members.
    assert!(f.org);
    let graph:Uuid=sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind) VALUES($1,'Windowed merge guard fixture','team') RETURNING id").bind(f.account).fetch_one(&f.owner).await.unwrap();
    let team:Uuid=sqlx::query_scalar("INSERT INTO teams(account_id,graph_id,name,created_by) VALUES($1,$2,'Windowed merge fixture',$3) RETURNING id").bind(f.account).bind(graph).bind(f.actor).fetch_one(&f.owner).await.unwrap();
    for actor in [f.actor, f.other_actor] {
        sqlx::query("INSERT INTO team_members(team_id,user_id,role) VALUES($1,$2,'member')")
            .bind(team)
            .bind(actor)
            .execute(&f.owner)
            .await
            .unwrap();
    }
    sqlx::query("INSERT INTO team_seat_grants(account_id,team_id,user_id,source,history_from) VALUES($1,$2,$3,'fixture:merge-windowed','2026-01-01T00:00:00Z'),($1,$2,$4,'fixture:merge-full-history',NULL)")
        .bind(f.account).bind(team).bind(f.actor).bind(f.other_actor).execute(&f.owner).await.unwrap();
    let visible = f
        .atom(graph, "recent anonymous selected correction support")
        .await;
    let hidden = f
        .atom(
            graph,
            "Window Merge Identity has independent older literal support.",
        )
        .await;
    let keep_atom = f
        .atom(graph, "Window Merge Keeper has recent literal support.")
        .await;
    sqlx::query("UPDATE atoms SET timestamp=CASE WHEN atom_id=$2 THEN '2025-12-01T00:00:00Z'::timestamptz ELSE '2026-02-01T00:00:00Z'::timestamptz END WHERE atom_id IN($1,$2,$3)")
        .bind(visible).bind(hidden).bind(keep_atom).execute(&f.owner).await.unwrap();
    let dup = f
        .node(
            graph,
            "before windowed merge correction",
            &[visible, hidden],
        )
        .await;
    let keep = f.node(graph, "Window Merge Keeper", &[keep_atom]).await;
    let mut tx = f.tx().await;
    patch_supported(
        &f.store,
        &mut tx,
        f.account,
        f.actor,
        dup,
        "Window Merge Identity",
        None,
        None,
        visible,
    )
    .await
    .unwrap();
    let annotation_visible:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM reef_identity_corrections WHERE node_id=$1 AND state='active')").bind(dup).fetch_one(&mut *tx).await.unwrap();
    assert!(annotation_visible);
    let hidden_visible: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM atoms WHERE atom_id=$1)")
            .bind(hidden)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    assert!(!hidden_visible);
    let literal_before: Option<String> =
        sqlx::query_scalar("SELECT public.library_node_title_before_reef_identity($1,$2)")
            .bind(graph)
            .bind(dup)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    assert_eq!(literal_before, None);
    tx.commit().await.unwrap();
    let before = merge_state_snapshot(f, graph)
        .await
        .to_string()
        .into_bytes();
    let mut tx = f.tx().await;
    let error = f
        .store
        .merge_nodes_scoped(&mut tx, keep, dup)
        .await
        .expect_err("windowed annotated merge with hidden support must stop before mutation");
    match error {
        StoreError::Conflict(message) => assert_eq!(message, "merge identity support unavailable"),
        _ => panic!("expected generic merge identity support Conflict"),
    }
    tx.commit().await.unwrap();
    assert_eq!(before,merge_state_snapshot(f,graph).await.to_string().into_bytes(),"caught merge guard followed by outer COMMIT changes no node, membership, correction, sync event or cached merge");
    let mut full = f.tx_as(f.account, f.other_actor).await;
    let literal_before: Option<String> =
        sqlx::query_scalar("SELECT public.library_node_title_before_reef_identity($1,$2)")
            .bind(graph)
            .bind(dup)
            .fetch_one(&mut *full)
            .await
            .unwrap();
    assert_eq!(literal_before.as_deref(), Some("Window Merge Identity"));
    f.store
        .merge_nodes_scoped(&mut full, keep, dup)
        .await
        .expect("fully admitted reader can merge independently supported annotated duplicate");
    full.commit().await.unwrap();
    let state: (String, Value) = sqlx::query_as("SELECT title,body FROM nodes WHERE node_id=$1")
        .bind(keep)
        .fetch_one(&f.owner)
        .await
        .unwrap();
    assert_eq!(state.0, "Window Merge Keeper");
    assert!(
        state.1["aliases"]
            .as_array()
            .unwrap()
            .iter()
            .any(|alias| alias == "window merge identity"),
        "independently supported B remains learned alias for fully admitted merge"
    );
    let cached: Value =
        sqlx::query_scalar("SELECT dup_snapshot FROM node_merges WHERE graph_id=$1 AND dup_id=$2")
            .bind(graph)
            .bind(dup)
            .fetch_one(&f.owner)
            .await
            .unwrap();
    assert_eq!(
        cached["title"], "Window Merge Identity",
        "fully admitted merge retains independently supported B history"
    );
    let unannotated_visible = f
        .atom(
            graph,
            "Ordinary Unannotated Duplicate has recent literal support.",
        )
        .await;
    let unannotated_hidden = f
        .atom(graph, "unrelated older hidden ordinary member")
        .await;
    let ordinary_keep_atom = f
        .atom(graph, "Ordinary Window Keeper has recent literal support.")
        .await;
    sqlx::query("UPDATE atoms SET timestamp=CASE WHEN atom_id=$2 THEN '2025-12-01T00:00:00Z'::timestamptz ELSE '2026-02-01T00:00:00Z'::timestamptz END WHERE atom_id IN($1,$2,$3)")
        .bind(unannotated_visible).bind(unannotated_hidden).bind(ordinary_keep_atom).execute(&f.owner).await.unwrap();
    let ordinary_dup = f
        .node(
            graph,
            "Ordinary Unannotated Duplicate",
            &[unannotated_visible, unannotated_hidden],
        )
        .await;
    let ordinary_keep = f
        .node(graph, "Ordinary Window Keeper", &[ordinary_keep_atom])
        .await;
    let mut ordinary = f.tx().await;
    let correction: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM reef_identity_corrections WHERE node_id=$1)",
    )
    .bind(ordinary_dup)
    .fetch_one(&mut *ordinary)
    .await
    .unwrap();
    assert!(!correction);
    let hidden_visible: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM atoms WHERE atom_id=$1)")
            .bind(unannotated_hidden)
            .fetch_one(&mut *ordinary)
            .await
            .unwrap();
    assert!(!hidden_visible);
    ordinary.commit().await.unwrap();
    assert_merge_admission_refused(f, graph, ordinary_keep, ordinary_dup).await;
    let mut admitted = f.tx_as(f.account, f.other_actor).await;
    f.store
        .merge_nodes_scoped(&mut admitted, ordinary_keep, ordinary_dup)
        .await
        .expect("fully admitted ordinary unannotated merge remains supported");
    admitted.commit().await.unwrap();
    let removed: bool =
        sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM nodes WHERE node_id=$1)")
            .bind(ordinary_dup)
            .fetch_one(&f.owner)
            .await
            .unwrap();
    assert!(removed);
    let hidden_selected = f
        .atom(graph, "older anonymous selected interpretation support")
        .await;
    let visible_member = f.atom(graph, "recent unrelated annotation member").await;
    let hidden_keep_atom = f
        .atom(
            graph,
            "Hidden Annotation Keeper has recent literal support.",
        )
        .await;
    sqlx::query("UPDATE atoms SET timestamp=CASE WHEN atom_id=$1 THEN '2025-12-01T00:00:00Z'::timestamptz ELSE '2026-02-01T00:00:00Z'::timestamptz END WHERE atom_id IN($1,$2,$3)")
        .bind(hidden_selected).bind(visible_member).bind(hidden_keep_atom).execute(&f.owner).await.unwrap();
    let hidden_dup = f
        .node(
            graph,
            "before entirely hidden correction",
            &[hidden_selected, visible_member],
        )
        .await;
    let hidden_keep = f
        .node(graph, "Hidden Annotation Keeper", &[hidden_keep_atom])
        .await;
    let mut full = f.tx_as(f.account, f.other_actor).await;
    patch_supported(
        &f.store,
        &mut full,
        f.account,
        f.other_actor,
        hidden_dup,
        "Entirely Hidden Merge Annotation",
        None,
        None,
        hidden_selected,
    )
    .await
    .unwrap();
    full.commit().await.unwrap();
    let mut windowed = f.tx().await;
    let hidden_correction: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM reef_identity_corrections WHERE node_id=$1)",
    )
    .bind(hidden_dup)
    .fetch_one(&mut *windowed)
    .await
    .unwrap();
    assert!(
        !hidden_correction,
        "selected-support RLS must hide the entire event from the merging reader"
    );
    let links: i64 = sqlx::query_scalar("SELECT count(*) FROM node_atoms WHERE node_id=$1")
        .bind(hidden_dup)
        .fetch_one(&mut *windowed)
        .await
        .unwrap();
    let visible_atoms: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM atoms a JOIN node_atoms l ON l.atom_id=a.atom_id WHERE l.node_id=$1",
    )
    .bind(hidden_dup)
    .fetch_one(&mut *windowed)
    .await
    .unwrap();
    assert_eq!(
        (links, visible_atoms),
        (2, 1),
        "actual graph-level link policy enumerates the member excluded by atom window"
    );
    windowed.commit().await.unwrap();
    assert_merge_admission_refused(f, graph, hidden_keep, hidden_dup).await;
    assert_merge_admission_refused(f, graph, hidden_dup, hidden_keep).await;
    append(
        &f.journal,
        &json!({"event":"tenant_windowed_merge_guard_passed","guarded_outer_commit_unchanged":true,"fully_admitted_annotated_merge_passed":true,"unannotated_hidden_member_merge_denied":true,"fully_admitted_unannotated_merge_passed":true,"entirely_hidden_correction_denied":true,"both_endpoint_orientations_denied":true,"error":"merge identity support unavailable","scope":"any RLS-unreadable member on either endpoint; no correction visibility prerequisite"}),
    );
}

async fn assert_merge_admission_refused(f: &Fixture, graph: Uuid, keep: Uuid, dup: Uuid) {
    let before = merge_state_snapshot(f, graph)
        .await
        .to_string()
        .into_bytes();
    let mut tx = f.tx().await;
    let error = f
        .store
        .merge_nodes_scoped(&mut tx, keep, dup)
        .await
        .expect_err("unreadable member on either endpoint must deny merge before mutation");
    match error {
        StoreError::Conflict(message) => assert_eq!(message, "merge identity support unavailable"),
        _ => panic!("expected generic merge identity support Conflict"),
    }
    tx.commit().await.unwrap();
    assert_eq!(before,merge_state_snapshot(f,graph).await.to_string().into_bytes(),"generic denial plus outer COMMIT must preserve exact nodes, links, corrections, sync and merge history");
}

async fn retained_admission_merge_denials(f: &Fixture) {
    for access in ["restricted", "unknown", "mixed"] {
        let permitted = f
            .atom(
                f.graph,
                &format!("{access} Merge Duplicate has independent permitted literal support."),
            )
            .await;
        let denied = f
            .atom(
                f.graph,
                &format!("{access} non-admitted legacy merge member"),
            )
            .await;
        if access == "mixed" {
            f.source(denied, "permitted").await;
            f.source(denied, "restricted").await;
        } else {
            f.source(denied, access).await;
        }
        let duplicate = f
            .node(
                f.graph,
                &format!("{access} Merge Duplicate"),
                &[permitted, denied],
            )
            .await;
        let keeper_atom = f
            .atom(
                f.graph,
                &format!("{access} Merge Keeper has permitted literal support."),
            )
            .await;
        let keeper = f
            .node(f.graph, &format!("{access} Merge Keeper"), &[keeper_atom])
            .await;
        let mut tx = f.tx().await;
        let readable: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM atoms WHERE atom_id=$1)")
                .bind(denied)
                .fetch_one(&mut *tx)
                .await
                .unwrap();
        assert!(
            readable,
            "{access}: ordinary atom SELECT alone must not be mistaken for Library admission"
        );
        let admitted: bool = sqlx::query_scalar("SELECT public.library_atom_permitted($1,$2)")
            .bind(f.graph)
            .bind(denied)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        assert!(!admitted);
        tx.commit().await.unwrap();
        assert_merge_admission_refused(f, f.graph, keeper, duplicate).await;
        assert_merge_admission_refused(f, f.graph, duplicate, keeper).await;
    }
    for access in ["restricted", "unknown"] {
        let permitted = f
            .atom(
                f.graph,
                &format!("{access} Semantic Duplicate has permitted literal support."),
            )
            .await;
        let source_atom = f
            .atom(f.graph, "retained source for denied semantic merge member")
            .await;
        let (_, version) = f.source(source_atom, access).await;
        let semantic:Uuid=sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model,entities) VALUES($1,$2,0,'fact','non-admitted semantic merge evidence','source-atoms-v1','deterministic-fixture','[]') RETURNING semantic_atom_id").bind(f.graph).bind(version).fetch_one(&f.owner).await.unwrap();
        let duplicate = f
            .node(
                f.graph,
                &format!("{access} Semantic Duplicate"),
                &[permitted],
            )
            .await;
        sqlx::query(
            "INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)",
        )
        .bind(f.graph)
        .bind(duplicate)
        .bind(semantic)
        .execute(&f.owner)
        .await
        .unwrap();
        let keeper_atom = f
            .atom(
                f.graph,
                &format!("{access} Semantic Keeper has permitted literal support."),
            )
            .await;
        let keeper = f
            .node(
                f.graph,
                &format!("{access} Semantic Keeper"),
                &[keeper_atom],
            )
            .await;
        let mut tx = f.tx().await;
        let raw_visible: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM semantic_atoms WHERE semantic_atom_id=$1)",
        )
        .bind(semantic)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
        assert!(
            raw_visible,
            "graph-visible semantic row still needs its retained source admission"
        );
        let admitted:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM semantic_atoms a JOIN source_versions v ON v.version_id=a.source_version_id AND v.graph_id=a.graph_id JOIN source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id WHERE a.graph_id=$1 AND a.semantic_atom_id=$2 AND s.access_state='permitted')").bind(f.graph).bind(semantic).fetch_one(&mut *tx).await.unwrap();
        assert!(!admitted);
        tx.commit().await.unwrap();
        assert_merge_admission_refused(f, f.graph, keeper, duplicate).await;
        assert_merge_admission_refused(f, f.graph, duplicate, keeper).await;
    }
    append(
        &f.journal,
        &json!({"event":"retained_admission_merge_denials_passed","tenant":f.org,"legacy_source_states":["restricted","unknown","mixed_permitted_restricted"],"semantic_source_states":["restricted","unknown"],"both_endpoint_orientations":true,"caught_error_outer_commit_state_unchanged":true,"error":"merge identity support unavailable"}),
    );
}

async fn private_graph_erasure(f: &Fixture) {
    let graph:Uuid=sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'Private graph erasure fixture','member',$2) RETURNING id").bind(f.account).bind(f.actor).fetch_one(&f.owner).await.unwrap();
    let support = f.atom(graph, "private anonymous retained source").await;
    let node = f
        .node(graph, "initial private graph label", &[support])
        .await;
    let mut tx = f.tx().await;
    patch_supported(
        &f.store,
        &mut tx,
        f.account,
        f.actor,
        node,
        "Private Graph Erased Identity",
        None,
        None,
        support,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let mut tx = f.tx().await;
    sqlx::query("DELETE FROM graphs WHERE id=$1")
        .bind(graph)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let leaks:i64=sqlx::query_scalar("SELECT (SELECT count(*) FROM reef_identity_corrections WHERE graph_id=$1 AND(label IS NOT NULL OR recorded_by IS NOT NULL))+(SELECT count(*) FROM op_log WHERE graph_id=$1 AND payload::text LIKE '%Private Graph Erased Identity%')+(SELECT count(*) FROM node_merges WHERE graph_id=$1 AND dup_snapshot::text LIKE '%Private Graph Erased Identity%')").bind(graph).fetch_one(&f.owner).await.unwrap();
    assert_eq!(leaks, 0);
    let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM graphs WHERE id=$1)")
        .bind(graph)
        .fetch_one(&f.owner)
        .await
        .unwrap();
    assert!(!exists);
}

async fn independent_history_preservation(f: &Fixture) {
    let old_label = "Historical \"Alpha\" \\ Identity";
    let new_label = "Independent Beta Identity";
    let old_support = f
        .atom(f.graph, "anonymous superseded interpretation evidence")
        .await;
    let new_support = f
        .atom(
            f.graph,
            "independently sufficient retained interpretation evidence",
        )
        .await;
    let node = f
        .node(
            f.graph,
            "before independent history fixture",
            &[old_support, new_support],
        )
        .await;
    f.rename(node, old_label, old_support).await;
    f.rename(node, new_label, new_support).await;
    let retained = json!({"summary":"Independent Beta summary remains","human_review_status":"confirmed","metadata":{"independent":"retained metadata","counter":7},"aliases":[new_label,old_label],"old_nested":{"label":old_label},"nested_array":[{"retained":"Independent Beta detail"},{"old":old_label}],"escaped_json":json!({"inner":old_label}).to_string()});
    sqlx::query("UPDATE nodes SET body=$2 WHERE node_id=$1")
        .bind(node)
        .bind(&retained)
        .execute(&f.owner)
        .await
        .unwrap();
    let mut tx = f.tx().await;
    f.store
        .patch_node_scoped(&mut tx, node, None, Some("thread"), None)
        .await
        .unwrap()
        .unwrap();
    tx.commit().await.unwrap();
    for string_body in [false, true] {
        sqlx::query("INSERT INTO node_merges(graph_id,keep_id,dup_id,dup_snapshot) SELECT graph_id,$2,node_id,jsonb_set(to_jsonb(nodes),'{body}',$3) FROM nodes WHERE node_id=$1")
            .bind(node).bind(Uuid::new_v4()).bind(if string_body{json!(retained.to_string())}else{retained.clone()}).execute(&f.owner).await.unwrap();
    }
    let mut tx = f.tx().await;
    sqlx::query("DELETE FROM atoms WHERE atom_id=$1")
        .bind(old_support)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        f.title(node).await.as_deref(),
        Some(new_label),
        "independent B authority survives old A support loss"
    );
    let current: Value = sqlx::query_scalar("SELECT body FROM nodes WHERE node_id=$1")
        .bind(node)
        .fetch_one(&f.owner)
        .await
        .unwrap();
    assert_preserved_independent_body(&current, old_label);
    let snapshots: Vec<Value> = sqlx::query_scalar(
        "SELECT dup_snapshot FROM node_merges WHERE dup_id=$1 ORDER BY merge_id",
    )
    .bind(node)
    .fetch_all(&f.owner)
    .await
    .unwrap();
    assert_eq!(snapshots.len(), 2);
    for snapshot in snapshots {
        assert_eq!(snapshot["title"], new_label);
        assert_preserved_independent_body(&snapshot["body"], old_label);
    }
    let history:Vec<Value>=sqlx::query_scalar("SELECT payload FROM op_log WHERE graph_id=$1 AND entity='node' AND entity_id=$2::text ORDER BY seq").bind(f.graph).bind(node).fetch_all(&f.owner).await.unwrap();
    assert!(history.iter().any(|entry| entry["title"] == new_label
        && decode_body(&entry["body"])["summary"] == "Independent Beta summary remains"));
    for entry in history {
        assert!(
            !entry.to_string().contains("Alpha"),
            "escaped or nested old A label survived in sync history: {entry}"
        );
        if entry["title"] == new_label && decode_body(&entry["body"]).get("summary").is_some() {
            assert_preserved_independent_body(&entry["body"], old_label);
        }
    }
    let old_content:i64=sqlx::query_scalar("SELECT count(*) FROM reef_identity_corrections WHERE node_id=$1 AND support_atom_id=$2 AND(label IS NOT NULL OR recorded_by IS NOT NULL)").bind(node).bind(old_support).fetch_one(&f.owner).await.unwrap();
    assert_eq!(old_content, 0);
    f.assert_hit(new_label, node, new_support).await;
}

fn decode_body(body: &Value) -> Value {
    if let Some(text) = body.as_str() {
        serde_json::from_str(text).expect("body JSON string remains valid")
    } else {
        body.clone()
    }
}
fn assert_preserved_independent_body(body: &Value, old_label: &str) {
    let body = decode_body(body);
    assert_eq!(body["summary"], "Independent Beta summary remains");
    assert_eq!(body["human_review_status"], "confirmed");
    assert_eq!(
        body["metadata"],
        json!({"independent":"retained metadata","counter":7})
    );
    assert!(
        !body.to_string().contains("Alpha"),
        "nested or escaped removed identity survived: {body}; old label={old_label}"
    );
}

// App-callable helpers are immutable transforms of supplied values, not stored
// cleanup entry points. The authority helpers remain denied by the catalog audit.
async fn sanitizer_value_contract(f: &Fixture) {
    let before = merge_state_snapshot(f, f.graph)
        .await
        .to_string()
        .into_bytes();
    let mut tx = f.tx().await;
    let audited: (i64, i64, i64) = sqlx::query_as("SELECT count(*),count(*) FILTER(WHERE p.prosecdef OR p.provolatile<>'i' OR NOT has_function_privilege(current_user,p.oid,'EXECUTE')),count(*) FILTER(WHERE EXISTS(SELECT 1 FROM aclexplode(COALESCE(p.proacl,acldefault('f',p.proowner))) a WHERE a.grantee=0 AND a.privilege_type='EXECUTE')) FROM pg_proc p WHERE p.pronamespace='public'::regnamespace AND p.proname IN('reef_identity_contains_label','reef_identity_sanitize_json','reef_identity_sanitize_body')")
        .fetch_one(&mut *tx).await.unwrap();
    assert_eq!(
        audited,
        (3, 0, 0),
        "only invoker immutable transforms, PUBLIC denied"
    );
    for (label, unrelated, explicit) in [
        ("Ann", "annual planning", "Ann approved this"),
        ("Jo", "major project", "with Jo today"),
        ("2026", "unrelated planning", "2026 approved this"),
        ("true", "truthful planning", "true approved this"),
        ("false", "falsely attributed", "false approved this"),
        ("null", "nullable metadata", "null approved this"),
        ("A[1].*", "A11xxxx unrelated", "with A[1].* today"),
        ("Zoë Δ", "preZoë Δpost", "with zoë δ today"),
        (
            r#"Name "Quoted" \ Label"#,
            "independent punctuation content",
            r#"with Name "Quoted" \ Label today"#,
        ),
    ] {
        let body = json!({"summary":unrelated,"metadata":{"unrelated":unrelated,"number":2026,"boolean":true,"empty":null},"aliases":[unrelated,label],"removed":explicit,(unrelated):"preserved-key",(explicit):"removed-key","encoded":json!({"removed":label,"retained":unrelated}).to_string()});
        let transformed: Value =
            sqlx::query_scalar("SELECT public.reef_identity_sanitize_body($1,ARRAY[$2])")
                .bind(&body)
                .bind(label)
                .fetch_one(&mut *tx)
                .await
                .unwrap();
        assert_eq!(transformed["summary"], unrelated);
        assert_eq!(
            transformed["metadata"],
            json!({"unrelated":unrelated,"number":2026,"boolean":true,"empty":null})
        );
        assert_eq!(transformed["aliases"], json!([unrelated]));
        assert_eq!(transformed[unrelated], "preserved-key");
        assert!(transformed.get("removed").is_none());
        assert!(transformed.get(explicit).is_none());
        assert_eq!(
            decode_body(&transformed["encoded"]),
            json!({"retained":unrelated})
        );
        let untouched = format!(
            "{{ \"summary\" : {} }}",
            serde_json::to_string(unrelated).unwrap()
        );
        let preserved: Value =
            sqlx::query_scalar("SELECT public.reef_identity_sanitize_body($1,ARRAY[$2])")
                .bind(json!(&untouched))
                .bind(label)
                .fetch_one(&mut *tx)
                .await
                .unwrap();
        assert_eq!(
            preserved,
            json!(untouched),
            "unchanged serialized B is byte-identical"
        );
    }
    tx.commit().await.unwrap();
    assert_eq!(
        before,
        merge_state_snapshot(f, f.graph)
            .await
            .to_string()
            .into_bytes(),
        "pure caller-value transforms cannot mutate retained rows"
    );

    // Actual normal-role loss invokes the same matcher on current and history.
    let selected = f
        .atom(f.graph, "anonymous boundary correction support")
        .await;
    let literal = f
        .atom(f.graph, "Boundary Retained Beta is independently attested.")
        .await;
    let node = f
        .node(f.graph, "before boundary fixture", &[selected, literal])
        .await;
    f.rename(node, "Ann", selected).await;
    let body = json!({"summary":"annual planning","annual planning":"preserved-key","aliases":["Ann","Boundary Retained Beta"],"metadata":{"project":"major project"},"old":"with Ann today"});
    sqlx::query("UPDATE nodes SET title='Boundary Retained Beta',body=$2 WHERE node_id=$1")
        .bind(node)
        .bind(&body)
        .execute(&f.owner)
        .await
        .unwrap();
    let mut tx = f.tx().await;
    f.store
        .patch_node_scoped(&mut tx, node, None, Some("thread"), None)
        .await
        .unwrap()
        .unwrap();
    tx.commit().await.unwrap();
    sqlx::query("INSERT INTO node_merges(graph_id,keep_id,dup_id,dup_snapshot) SELECT graph_id,$2,node_id,to_jsonb(nodes) FROM nodes WHERE node_id=$1").bind(node).bind(Uuid::new_v4()).execute(&f.owner).await.unwrap();
    let mut tx = f.tx().await;
    sqlx::query("DELETE FROM public.atoms WHERE atom_id=$1")
        .bind(selected)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let current: Value = sqlx::query_scalar("SELECT body FROM nodes WHERE node_id=$1")
        .bind(node)
        .fetch_one(&f.owner)
        .await
        .unwrap();
    let snapshot: Value =
        sqlx::query_scalar("SELECT dup_snapshot FROM node_merges WHERE dup_id=$1")
            .bind(node)
            .fetch_one(&f.owner)
            .await
            .unwrap();
    let history: Vec<Value> =
        sqlx::query_scalar("SELECT payload FROM op_log WHERE entity='node' AND entity_id=$1::text")
            .bind(node)
            .fetch_all(&f.owner)
            .await
            .unwrap();
    for body in std::iter::once(current)
        .chain(std::iter::once(decode_body(&snapshot["body"])))
        .chain(
            history
                .iter()
                .filter(|p| p["title"] == "Boundary Retained Beta")
                .map(|p| decode_body(&p["body"])),
        )
    {
        assert_eq!(body["summary"], "annual planning");
        assert_eq!(body["annual planning"], "preserved-key");
        assert_eq!(body["metadata"], json!({"project":"major project"}));
        assert_eq!(body["aliases"], json!(["Boundary Retained Beta"]));
        assert!(body.get("old").is_none());
    }
}

async fn merge_cached_history_preservation(f: &Fixture) {
    for current_is_human in [true, false] {
        let old_label = if current_is_human {
            "Historic Secret Alpha"
        } else {
            "Historic Secret Gamma"
        };
        let independent = if current_is_human {
            "Independent Alias Beta"
        } else {
            "Independent Literal Delta"
        };
        let support = f
            .atom(f.graph, "anonymous historical identity interpretation")
            .await;
        let literal = f
            .atom(
                f.graph,
                &format!("{independent} is independently attested."),
            )
            .await;
        let dup = f
            .node(
                f.graph,
                "before cached history fixture",
                &[support, literal],
            )
            .await;
        f.rename(dup, old_label, support).await;
        if !current_is_human {
            let mut tx = f.tx().await;
            f.store
                .patch_node_scoped(&mut tx, dup, Some(independent), None, None)
                .await
                .unwrap()
                .unwrap();
            tx.commit().await.unwrap();
        }
        let retained = json!({"aliases":[old_label,independent],"digest":format!("{old_label} private prose"),"summary":"Independent cached summary remains","metadata":{"unrelated":"preserved","counter":7},"encoded":json!({"old":old_label,"retained":"Independent detail"}).to_string()});
        sqlx::query("UPDATE nodes SET body=$2 WHERE node_id=$1")
            .bind(dup)
            .bind(&retained)
            .execute(&f.owner)
            .await
            .unwrap();
        let keeper_title = format!("Cached Keeper {}", Uuid::new_v4());
        let keeper_atom = f
            .atom(f.graph, &format!("{keeper_title} has literal evidence."))
            .await;
        let keep = f.node(f.graph, &keeper_title, &[keeper_atom]).await;
        let mut tx = f.tx().await;
        f.store
            .merge_nodes_scoped(&mut tx, keep, dup)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let keeper: Value = sqlx::query_scalar("SELECT body FROM nodes WHERE node_id=$1")
            .bind(keep)
            .fetch_one(&f.owner)
            .await
            .unwrap();
        assert!(keeper["aliases"]
            .as_array()
            .unwrap()
            .contains(&json!(independent.to_lowercase())));
        let snapshot: Value =
            sqlx::query_scalar("SELECT dup_snapshot FROM node_merges WHERE dup_id=$1")
                .bind(dup)
                .fetch_one(&f.owner)
                .await
                .unwrap();
        let body = decode_body(&snapshot["body"]);
        assert_eq!(body["summary"], "Independent cached summary remains");
        assert_eq!(
            body["metadata"],
            json!({"unrelated":"preserved","counter":7})
        );
        assert_eq!(body["aliases"], json!([independent]));
        assert_eq!(
            decode_body(&body["encoded"]),
            json!({"retained":"Independent detail"})
        );
        assert!(body.get("digest").is_none());
        if !current_is_human {
            assert_eq!(snapshot["title"], independent);
        }
        let history:Vec<Value>=sqlx::query_scalar("SELECT payload FROM op_log WHERE graph_id=$1 AND entity='node' AND entity_id IN($2::text,$3::text)").bind(f.graph).bind(keep).bind(dup).fetch_all(&f.owner).await.unwrap();
        assert!(!keeper.to_string().contains(old_label));
        assert!(!snapshot.to_string().contains(old_label));
        assert!(
            history.iter().all(|p| !p.to_string().contains(old_label)),
            "historical unsupported A cannot re-enter through cached aliases/delete/undo"
        );
        let deletes:Vec<Value>=sqlx::query_scalar("SELECT payload FROM op_log WHERE graph_id=$1 AND entity='node' AND entity_id=$2::text AND op_type='delete'").bind(f.graph).bind(dup).fetch_all(&f.owner).await.unwrap();
        assert_eq!(deletes.len(), 1);
        assert_eq!(
            decode_body(&deletes[0]["body"])["metadata"],
            json!({"unrelated":"preserved","counter":7})
        );
    }
}

async fn purge_queue_authority_and_delivery(f: &Fixture) {
    for table in ["source_purge_events", "source_purge_dependencies"] {
        for privilege in [
            "INSERT",
            "UPDATE",
            "DELETE",
            "TRUNCATE",
            "TRIGGER",
            "REFERENCES",
        ] {
            let has: bool = sqlx::query_scalar("SELECT has_table_privilege(current_user,$1,$2)")
                .bind(format!("public.{table}"))
                .bind(privilege)
                .fetch_one(&f.app)
                .await
                .unwrap();
            assert!(
                !has,
                "normal role unexpectedly holds {privilege} on {table}"
            );
        }
        for privilege in ["INSERT", "UPDATE", "REFERENCES"] {
            let has: bool =
                sqlx::query_scalar("SELECT has_any_column_privilege(current_user,$1,$2)")
                    .bind(format!("public.{table}"))
                    .bind(privilege)
                    .fetch_one(&f.app)
                    .await
                    .unwrap();
            assert!(
                !has,
                "normal role unexpectedly holds column {privilege} on {table}"
            );
        }
        let readable: bool =
            sqlx::query_scalar("SELECT has_table_privilege(current_user,$1,'SELECT')")
                .bind(format!("public.{table}"))
                .fetch_one(&f.app)
                .await
                .unwrap();
        assert!(readable);
    }
    let support = f
        .atom(
            f.graph,
            "legitimate source purge and normal worker drain evidence",
        )
        .await;
    let (item, _) = f.source(support, "permitted").await;
    let node = f
        .node(f.graph, "queue lifecycle initial label", &[support])
        .await;
    f.rename(node, "Legitimate Queue Identity", support).await;
    let mut tx = f.tx().await;
    let forged=sqlx::query("INSERT INTO source_purge_events(account_id,graph_id,source_item_id,event_kind) VALUES($1,$2,$3,'deleted')").bind(f.account).bind(f.graph).bind(item).execute(&mut *tx).await;
    assert!(
        forged.is_err(),
        "untrusted caller cannot manufacture an erasure event"
    );
    tx.rollback().await.unwrap();
    let mut tx = f.tx().await;
    sqlx::query("DELETE FROM source_items WHERE source_item_id=$1")
        .bind(item)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let event:Uuid=sqlx::query_scalar("SELECT event_id FROM source_purge_events WHERE graph_id=$1 AND source_item_id=$2 AND event_kind='deleted'").bind(f.graph).bind(item).fetch_one(&f.owner).await.unwrap();
    let legitimate:i64=sqlx::query_scalar("SELECT count(*) FROM source_purge_dependencies WHERE event_id=$1 AND kind='atom' AND object_id=$2").bind(event).bind(support).fetch_one(&f.owner).await.unwrap();
    assert_eq!(
        legitimate, 1,
        "actual source delete trigger must still enqueue dependencies"
    );
    let mut tx = f.tx().await;
    let forged=sqlx::query("INSERT INTO source_purge_dependencies(event_id,kind,graph_id,object_id) VALUES($1,'atom',$2,$3)").bind(event).bind(f.graph).bind(Uuid::new_v4()).execute(&mut *tx).await;
    assert!(
        forged.is_err(),
        "untrusted caller cannot graft objects onto a real erasure event"
    );
    tx.rollback().await.unwrap();
    let mut tx = if f.org {
        coralswarm_backend::tenancy::scope::begin_tenant_system_scoped(&f.app, f.account)
            .await
            .unwrap()
    } else {
        coralswarm_backend::tenancy::scope::begin_account_scoped(&f.app, f.account)
            .await
            .unwrap()
    };
    let mut drained = 0_i32;
    for _ in 0..4 {
        let count: i32 = sqlx::query_scalar("SELECT public.library_drain_purge(100)")
            .fetch_one(&mut *tx)
            .await
            .expect("trusted normal-role scoped purge drain survives queue ACL hardening");
        drained += count;
        if count == 0 {
            break;
        }
    }
    tx.commit().await.unwrap();
    assert!(drained > 0);
    let removed: bool =
        sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM atoms WHERE atom_id=$1)")
            .bind(support)
            .fetch_one(&f.owner)
            .await
            .unwrap();
    assert!(removed);
    let delivered:bool=sqlx::query_scalar("SELECT processed_at IS NOT NULL FROM source_purge_dependencies WHERE event_id=$1 AND kind='atom' AND object_id=$2").bind(event).bind(support).fetch_one(&f.owner).await.unwrap();
    assert!(delivered);
    append(
        &f.journal,
        &json!({"event":"normal_role_scoped_purge_drain_passed","tenant":f.org,"processed":drained,"forged_event_denied":true,"forged_dependency_denied":true}),
    );
}

async fn cross_account_and_copy(f: &Fixture) {
    let copied_graph: Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'Copied graph fixture','member',$2) RETURNING id").bind(f.account).bind(f.actor).fetch_one(&f.owner).await.unwrap();
    let atom = f
        .atom(copied_graph, "Anonymous record with no named person.")
        .await;
    let copied = f
        .node(copied_graph, "Copied Interpreted Identity", &[atom])
        .await;
    sqlx::query("UPDATE nodes SET body=jsonb_build_object('aliases',jsonb_build_array('Copied Interpreted Identity'),'digest','Copied Interpreted Identity') WHERE node_id=$1").bind(copied).execute(&f.owner).await.unwrap();
    let mut tx = f.tx().await;
    let safe: Option<String> = sqlx::query_scalar("SELECT public.library_node_title($1,$2)")
        .bind(copied_graph)
        .bind(copied)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert_eq!(
        safe, None,
        "imported metadata and copied labels confer no authority"
    );
    assert!(f
        .store
        .matched_reef_ids_scoped(
            &mut tx,
            copied_graph,
            "Copied Interpreted Identity",
            20,
            &RetrievalFilter::default()
        )
        .await
        .unwrap()
        .is_empty());
    // A metadata-only label supplies no authority, while admitted literal
    // context still provides ordinary lexical recall at the unchanged floor.
    let context_matches = f
        .store
        .matched_reef_ids_scoped(
            &mut tx,
            copied_graph,
            "anonymous record",
            20,
            &RetrievalFilter::default(),
        )
        .await
        .unwrap();
    assert!(
        context_matches.iter().any(|(id, _)| *id == copied),
        "individually admitted source context remains searchable"
    );
    tx.commit().await.unwrap();
    let shared:Uuid=sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind) VALUES($1,'Cross-account shared fixture','personal') RETURNING id").bind(f.other_account).fetch_one(&f.owner).await.unwrap();
    if !f.org {
        sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'member')")
            .bind(f.other_account)
            .bind(f.actor)
            .execute(&f.owner)
            .await
            .unwrap();
    }
    let support = f
        .atom(shared, "Read Only Literal Identity is retained evidence.")
        .await;
    let node = f
        .node(shared, "Read Only Literal Identity", &[support])
        .await;
    // Read in the target account; the later patch deliberately supplies
    // f.account to test caller/transaction account mismatch, not a reader role.
    let mut reader = f.tx_as(f.other_account, f.actor).await;
    let readable: Option<String> = sqlx::query_scalar("SELECT public.library_node_title($1,$2)")
        .bind(shared)
        .bind(node)
        .fetch_one(&mut *reader)
        .await
        .unwrap();
    assert_eq!(readable.as_deref(), Some("Read Only Literal Identity"));
    assert!(patch_supported(
        &f.store,
        &mut reader,
        f.account,
        f.actor,
        node,
        "Read Only Forgery",
        None,
        None,
        support
    )
    .await
    .is_err());
    reader.commit().await.unwrap();
}

async fn source_purge(f: &Fixture) {
    let support = f
        .atom(f.graph, "anonymous source slated for physical erasure")
        .await;
    let (item, _) = f.source(support, "permitted").await;
    let node = f
        .node(f.graph, "pre-purge source fixture", &[support])
        .await;
    f.rename(node, "Private Erasure Identity", support).await;
    let mut tx = f.tx().await;
    sqlx::query("DELETE FROM source_items WHERE source_item_id=$1")
        .bind(item)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(f.title(node).await, None);
    let leaks:i64=sqlx::query_scalar("SELECT (SELECT count(*) FROM reef_identity_corrections WHERE node_id=$1 AND(label IS NOT NULL OR recorded_by IS NOT NULL))+(SELECT count(*) FROM nodes WHERE node_id=$1 AND to_jsonb(nodes)::text LIKE '%Private Erasure Identity%')+(SELECT count(*) FROM op_log WHERE entity='node' AND entity_id=$1::text AND payload::text LIKE '%Private Erasure Identity%')").bind(node).fetch_one(&f.owner).await.unwrap();
    assert_eq!(
        leaks, 0,
        "source purge erases label/actor authority and sync history"
    );
}

async fn merge_and_shadow(f: &Fixture) {
    let keep_atom = f
        .atom(
            f.graph,
            "Stable Literal Keeper has independent literal support.",
        )
        .await;
    let keep = f.node(f.graph, "Stable Literal Keeper", &[keep_atom]).await;
    let dup_atom = f
        .atom(f.graph, "anonymous duplicate interpretation source")
        .await;
    let dup = f.node(f.graph, "pre-merge duplicate", &[dup_atom]).await;
    f.rename(dup, "Confidential Merge Identity", dup_atom).await;
    sqlx::query("UPDATE nodes SET body=jsonb_build_object('digest','Confidential Merge Identity','aliases',jsonb_build_array('Confidential Merge Identity')) WHERE node_id=$1").bind(dup).execute(&f.owner).await.unwrap();
    let mut tx = f.tx().await;
    f.store
        .merge_nodes_scoped(&mut tx, keep, dup)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        f.title(keep).await.as_deref(),
        Some("Stable Literal Keeper")
    );
    let contamination:i64=sqlx::query_scalar("SELECT (SELECT count(*) FROM nodes WHERE node_id=$1 AND to_jsonb(nodes)::text LIKE '%Confidential Merge Identity%')+(SELECT count(*) FROM op_log WHERE graph_id=$3 AND entity='node' AND entity_id IN($1::text,$2::text) AND payload::text LIKE '%Confidential Merge Identity%')+(SELECT count(*) FROM node_merges WHERE graph_id=$3 AND dup_id=$2 AND dup_snapshot::text LIKE '%Confidential Merge Identity%')").bind(keep).bind(dup).bind(f.graph).fetch_one(&f.owner).await.unwrap();
    assert_eq!(
        contamination, 0,
        "actual merge must not recontaminate aliases, delete op or cached snapshot"
    );
    f.assert_absent("Confidential Merge Identity", keep).await;
    let support = f
        .atom(
            f.graph,
            "anonymous support protected from temporary table shadowing",
        )
        .await;
    let node = f.node(f.graph, "before shadowing", &[support]).await;
    f.rename(node, "Safe Shadow Boundary", support).await;
    let mut tx = f.tx().await;
    for table in [
        "atoms",
        "node_atoms",
        "nodes",
        "source_items",
        "source_versions",
        "source_version_chunks",
        "reef_identity_corrections",
    ] {
        sqlx::query(&format!(
            "CREATE TEMP TABLE {table} (LIKE public.{table} INCLUDING DEFAULTS) ON COMMIT DROP"
        ))
        .execute(&mut *tx)
        .await
        .unwrap();
        sqlx::query(&format!(
            "INSERT INTO pg_temp.{table} SELECT * FROM public.{table}"
        ))
        .execute(&mut *tx)
        .await
        .unwrap();
    }
    sqlx::query("UPDATE pg_temp.nodes SET title='Shadow Forged Identity' WHERE node_id=$1")
        .bind(node)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("UPDATE pg_temp.atoms SET content_inline='Shadow Forged Identity shadowsecret' WHERE atom_id=$1").bind(support).execute(&mut *tx).await.unwrap();
    sqlx::query("UPDATE pg_temp.reef_identity_corrections SET label='Shadow Forged Identity' WHERE node_id=$1").bind(node).execute(&mut *tx).await.unwrap();
    let safe: Option<String> = sqlx::query_scalar("SELECT public.library_node_title($1,$2)")
        .bind(f.graph)
        .bind(node)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert_eq!(safe.as_deref(), Some("Safe Shadow Boundary"));
    let spoofed = f
        .store
        .matched_reef_ids_scoped(
            &mut tx,
            f.graph,
            "shadowsecret",
            20,
            &RetrievalFilter::default(),
        )
        .await
        .unwrap();
    assert!(!spoofed.iter().any(|(id, _)| *id == node));
    let hooks:Vec<String>=sqlx::query_scalar("SELECT proname::text FROM pg_proc WHERE pronamespace='public'::regnamespace AND proname LIKE 'reef_identity_%' AND prorettype='trigger'::regtype ORDER BY proname").fetch_all(&mut *tx).await.unwrap();
    assert_eq!(
        hooks.len(),
        8,
        "every reef identity trigger function is covered"
    );
    for hook in &hooks {
        let mut savepoint = tx.begin().await.unwrap();
        let attached=sqlx::query(&format!("CREATE TRIGGER spoof BEFORE UPDATE ON pg_temp.nodes FOR EACH ROW EXECUTE FUNCTION public.{hook}()"))
            .execute(&mut *savepoint).await;
        assert!(
            attached.is_err(),
            "normal role must not attach private cleanup hook {hook}"
        );
        savepoint.rollback().await.unwrap();
    }
    let mut savepoint = tx.begin().await.unwrap();
    let callable=sqlx::query("SELECT public.reef_identity_erase_support($1,NULL,NULL,NULL,'{}'::uuid[],'{}'::uuid[],false)").bind(f.graph).execute(&mut *savepoint).await;
    assert!(callable.is_err());
    savepoint.rollback().await.unwrap();
    sqlx::query("DELETE FROM public.atoms WHERE atom_id=$1")
        .bind(support)
        .execute(&mut *tx)
        .await
        .unwrap();
    let erased: Option<String> = sqlx::query_scalar("SELECT public.library_node_title($1,$2)")
        .bind(f.graph)
        .bind(node)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert_eq!(erased, None);
    tx.commit().await.unwrap();
    assert_eq!(f.title(node).await, None);
    let mut owner = f.owner.begin().await.unwrap();
    sqlx::query("CREATE TEMP TABLE reef_identity_wrong_physical(id integer) ON COMMIT DROP")
        .execute(&mut *owner)
        .await
        .unwrap();
    sqlx::query("INSERT INTO pg_temp.reef_identity_wrong_physical VALUES(1)")
        .execute(&mut *owner)
        .await
        .unwrap();
    for hook in &hooks {
        let mut savepoint = owner.begin().await.unwrap();
        sqlx::query(&format!("CREATE TRIGGER wrong_physical BEFORE UPDATE ON pg_temp.reef_identity_wrong_physical FOR EACH ROW EXECUTE FUNCTION public.{hook}()"))
            .execute(&mut *savepoint).await.expect("trusted test owner can attach owned trigger function");
        let error = sqlx::query("UPDATE pg_temp.reef_identity_wrong_physical SET id=id+1")
            .execute(&mut *savepoint)
            .await
            .expect_err("physical trigger guard must reject an owner-attached wrong relation");
        let error = error
            .as_database_error()
            .expect("trigger context rejection is a database error");
        assert_eq!(
            error.code().as_deref(),
            Some("42501"),
            "{hook}: guard rejects before record-field access"
        );
        assert!(
            error
                .message()
                .contains("invalid reef identity trigger context"),
            "{hook}: wrong trigger context must reach the explicit guard first: {}",
            error.message()
        );
        savepoint.rollback().await.unwrap();
    }
    let original: i32 = sqlx::query_scalar("SELECT id FROM pg_temp.reef_identity_wrong_physical")
        .fetch_one(&mut *owner)
        .await
        .unwrap();
    assert_eq!(original, 1);
    owner.commit().await.unwrap();
}

async fn membership_and_departure(f: &Fixture) {
    let team_graph:Uuid=sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind) VALUES($1,'Shared identity team','team') RETURNING id").bind(f.account).fetch_one(&f.owner).await.unwrap();
    let team:Uuid=sqlx::query_scalar("INSERT INTO teams(account_id,graph_id,name,created_by) VALUES($1,$2,'Identity fixture',$3) RETURNING id").bind(f.account).bind(team_graph).bind(f.actor).fetch_one(&f.owner).await.unwrap();
    if !f.org {
        sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'member')")
            .bind(f.account)
            .bind(f.other_actor)
            .execute(&f.owner)
            .await
            .unwrap();
    }
    for actor in [f.actor, f.other_actor] {
        sqlx::query("INSERT INTO team_members(team_id,user_id,role) VALUES($1,$2,'member')")
            .bind(team)
            .bind(actor)
            .execute(&f.owner)
            .await
            .unwrap();
    }
    let atom = f
        .atom(team_graph, "shared anonymous engineering record")
        .await;
    let node = f
        .node(team_graph, "before shared interpretation", &[atom])
        .await;
    let mut tx = f.tx().await;
    patch_supported(
        &f.store,
        &mut tx,
        f.account,
        f.actor,
        node,
        "Shared Durable Identity",
        None,
        None,
        atom,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    sqlx::query("DELETE FROM team_members WHERE team_id=$1 AND user_id=$2")
        .bind(team)
        .bind(f.actor)
        .execute(&f.owner)
        .await
        .unwrap();
    let mut departed = f.tx().await;
    let lost: Option<String> = sqlx::query_scalar("SELECT library_node_title($1,$2)")
        .bind(team_graph)
        .bind(node)
        .fetch_one(&mut *departed)
        .await
        .unwrap();
    assert_eq!(lost, None);
    assert!(patch_supported(
        &f.store,
        &mut departed,
        f.account,
        f.actor,
        node,
        "Departed Forgery",
        None,
        None,
        atom
    )
    .await
    .is_err());
    departed.commit().await.unwrap();
    let mut reader = f.tx_as(f.account, f.other_actor).await;
    let retained: Option<String> = sqlx::query_scalar("SELECT library_node_title($1,$2)")
        .bind(team_graph)
        .bind(node)
        .fetch_one(&mut *reader)
        .await
        .unwrap();
    assert_eq!(retained.as_deref(), Some("Shared Durable Identity"));
    let hits = f
        .store
        .matched_reef_ids_scoped(
            &mut reader,
            team_graph,
            "Shared Durable Identity",
            20,
            &RetrievalFilter::default(),
        )
        .await
        .unwrap();
    assert!(hits.iter().any(|(id, _)| *id == node));
    reader.commit().await.unwrap();
    let mut reader = f.tx_as(f.account, f.other_actor).await;
    sqlx::query("DELETE FROM atoms WHERE atom_id=$1")
        .bind(atom)
        .execute(&mut *reader)
        .await
        .unwrap();
    reader.commit().await.unwrap();
    let erased:i64=sqlx::query_scalar("SELECT count(*) FROM reef_identity_corrections WHERE node_id=$1 AND(label IS NOT NULL OR recorded_by IS NOT NULL)").bind(node).fetch_one(&f.owner).await.unwrap();
    assert_eq!(erased, 0);
}

async fn lexical_scale(org: bool) {
    let f = Fixture::new(org).await;
    sqlx::query("INSERT INTO atoms(atom_id,graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) SELECT md5('reef-scale-atom-'||i)::uuid,$1,'note','scale:'||i,'synthetic-reef-scale','Reef'||(i%1000)||' ubiquitous engineering material, lighthouse deployment reliability rollback planning.',now() FROM generate_series(0,9999) i").bind(f.graph).execute(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO nodes(node_id,graph_id,type,title,body) SELECT md5('reef-scale-node-'||i)::uuid,$1,'entity','Reef'||i,jsonb_build_object('digest','forbiddenrawdigest','aliases',jsonb_build_array('forbiddenrawalias')) FROM generate_series(0,999) i").bind(f.graph).execute(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO node_atoms(node_id,atom_id) SELECT md5('reef-scale-node-'||(i%1000))::uuid,md5('reef-scale-atom-'||i)::uuid FROM generate_series(0,9999) i").execute(&f.owner).await.unwrap();
    // Bound same-node counter-trigger work per seed statement without changing
    // the corpus, connection timeout, or measured query budgets below.
    let mut inserted_skew_links = 0;
    for first in (0_i32..6000).step_by(500) {
        inserted_skew_links += sqlx::query("INSERT INTO node_atoms(node_id,atom_id) SELECT md5('reef-scale-node-0')::uuid,md5('reef-scale-atom-'||i)::uuid FROM generate_series($1,$2) i ON CONFLICT DO NOTHING")
            .bind(first).bind(first + 499).execute(&f.owner).await.unwrap().rows_affected();
    }
    assert_eq!(inserted_skew_links, 5994, "six of the 6000 skew links already exist in the base corpus");
    let node: Uuid = sqlx::query_scalar("SELECT md5('reef-scale-node-0')::uuid")
        .fetch_one(&f.owner)
        .await
        .unwrap();
    let support: Uuid = sqlx::query_scalar("SELECT md5('reef-scale-atom-0')::uuid")
        .fetch_one(&f.owner)
        .await
        .unwrap();
    f.rename(node, "Anchor Meridian", support).await;
    let long_body = format!(
        "{} rarecontextlast",
        "extended inline deployment context ".repeat(1800)
    );
    let long = f.atom(f.graph, &long_body).await;
    sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)")
        .bind(node)
        .bind(long)
        .execute(&f.owner)
        .await
        .unwrap();
    let forbidden = f
        .atom(f.graph, "forbiddenlexicalsecret ubiquitous deployment")
        .await;
    f.source(forbidden, "restricted").await;
    sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)")
        .bind(node)
        .bind(forbidden)
        .execute(&f.owner)
        .await
        .unwrap();
    let foreign:Uuid=sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'Unrelated scale graph','member',$2) RETURNING id").bind(f.other_account).bind(f.other_actor).fetch_one(&f.owner).await.unwrap();
    let foreign_atom = f
        .atom(foreign, "Anchor Meridian unrelatedgraphsecret ubiquitous")
        .await;
    f.node(foreign, "Anchor Meridian", &[foreign_atom]).await;
    f.owner.execute("ANALYZE").await.unwrap();
    f.assert_hit("rarecontextlast", node, long).await;
    for denied in [
        "forbiddenlexicalsecret",
        "unrelatedgraphsecret",
        "forbiddenrawdigest",
        "forbiddenrawalias",
    ] {
        f.assert_absent(denied, node).await;
    }
    let giant = (0..90_000)
        .map(|i| format!("lexeme{i:06}"))
        .collect::<Vec<_>>()
        .join(",")
        + ",rarelastexeme";
    assert!(giant.split_whitespace().count() == 1);
    assert!(giant.len() > 1_000_000);
    let mut tx = f.tx().await;
    let giant_result:(i64,bool)=sqlx::query_as("SELECT count(*),bool_or(v @@ plainto_tsquery('english','rarelastexeme')) FROM reef_identity_text_vectors($1) v").bind(&giant).fetch_one(&mut *tx).await.unwrap();
    assert!(
        giant_result.0 > 1 && giant_result.1,
        "large comma-separated distinct lexemes must not overflow or lose the final term"
    );
    tx.commit().await.unwrap();
    for (class, query) in [
        ("name", "Anchor Meridian"),
        ("context", "lighthouse deployment reliability"),
        ("body", "rarecontextlast"),
        ("ubiquitous", "ubiquitous"),
        ("no_match", "unrecognizedxyzabsence"),
    ] {
        let mut latencies = Vec::new();
        let mut member_latencies = Vec::new();
        for repetition in 0..=30 {
            let mut tx = f.tx().await;
            let started = Instant::now();
            let reefs = tokio::time::timeout(
                Duration::from_secs(10),
                f.store.matched_reef_ids_scoped(
                    &mut tx,
                    f.graph,
                    query,
                    20,
                    &RetrievalFilter::default(),
                ),
            )
            .await
            .expect("reef query exceeded local 10-second guard")
            .unwrap();
            let elapsed = started.elapsed().as_secs_f64() * 1000.;
            if repetition > 0 {
                latencies.push(elapsed);
            }
            if class == "name" || class == "body" {
                assert!(reefs.iter().any(|(id, _)| *id == node));
            }
            if class == "no_match" {
                assert!(reefs.is_empty());
            }
            let member_started = Instant::now();
            let atoms = tokio::time::timeout(
                Duration::from_secs(10),
                f.store.keyword_search_node_atoms_scoped(
                    &mut tx,
                    f.graph,
                    query,
                    20,
                    &RetrievalFilter::default(),
                ),
            )
            .await
            .expect("full lexical member query exceeded local 10-second guard")
            .unwrap();
            let member_elapsed = member_started.elapsed().as_secs_f64() * 1000.;
            if repetition > 0 {
                member_latencies.push(member_elapsed);
            }
            assert!(!atoms
                .iter()
                .any(|(id, _)| *id == forbidden || *id == foreign_atom));
            if repetition == 0 {
                append(
                    &f.journal,
                    &json!({"event":"lexical_scale_cold","tenant":org,"class":class,"query_ms":elapsed,"reefs":reefs.len(),"members":atoms.len()}),
                );
                append(
                    &f.journal,
                    &json!({"event":"lexical_member_scale_cold","tenant":org,"class":class,"query_ms":member_elapsed,"members":atoms.len()}),
                );
            }
            tx.commit().await.unwrap();
        }
        latencies.sort_by(f64::total_cmp);
        let (p50, p95) = (latencies[14], latencies[28]);
        append(
            &f.journal,
            &json!({"event":"lexical_scale_warm","tenant":org,"class":class,"samples":30,"p50_ms":p50,"p95_ms":p95,"local_guard_ms":1000,"scope":"local regression guard, not service SLO"}),
        );
        assert!(
            p95 < 1000.,
            "{class} p95 {p95:.1}ms exceeded fixed local guard"
        );
        member_latencies.sort_by(f64::total_cmp);
        let (member_p50, member_p95) = (member_latencies[14], member_latencies[28]);
        append(
            &f.journal,
            &json!({"event":"lexical_member_scale_warm","tenant":org,"class":class,"samples":30,"p50_ms":member_p50,"p95_ms":member_p95,"local_guard_ms":1000,"scope":"full keyword_search_node_atoms_scoped local guard, not service SLO"}),
        );
        assert!(
            member_p95 < 1000.,
            "{class} full member query p95 {member_p95:.1}ms exceeded fixed local guard"
        );
        let tokens = node_query_tokens(query);
        let mut tx = f.tx().await;
        let plan: Value = sqlx::query_scalar(&format!(
            "EXPLAIN (ANALYZE,BUFFERS,VERBOSE,SETTINGS,FORMAT JSON) {}",
            reef_lexical_query()
        ))
        .bind(f.graph)
        .bind(&tokens)
        .bind(None::<String>)
        .bind(tokens.join(" | "))
        .bind((REEF_COVERAGE_FLOOR * tokens.len() as f64).ceil() as i64)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
        append(
            &f.journal,
            &json!({"event":"lexical_scale_production_plan","tenant":org,"class":class,"plan":plan}),
        );
        let selected = f.store.matched_reef_ids_scoped(
            &mut tx, f.graph, query, 20, &RetrievalFilter::default(),
        ).await.unwrap();
        let selected_ids: Vec<Uuid> = selected.iter().map(|(id, _)| *id).collect();
        let member_plan: Value = sqlx::query_scalar(&format!(
            "EXPLAIN (ANALYZE,BUFFERS,VERBOSE,SETTINGS,FORMAT JSON) {}",
            reef_member_lexical_query()
        ))
        .bind(&selected_ids)
        .bind(f.graph)
        .bind(tokens.join(" | "))
        .fetch_one(&mut *tx)
        .await
        .unwrap();
        append(
            &f.journal,
            &json!({"event":"lexical_member_scale_production_plan","tenant":org,"class":class,"selected_reef_ids":selected_ids,"plan":member_plan}),
        );
        tx.commit().await.unwrap();
    }
    append(
        &f.journal,
        &json!({"event":"lexical_scale_passed","tenant":org,"reefs":1000,"base_atoms":10000,"skew_links":6000}),
    );
    f.close().await;
}

#[tokio::test]
async fn reef_identity_consumer_security_and_lifecycle() {
    security_and_lifecycle(false).await;
}
#[tokio::test]
async fn reef_identity_tenant_security_and_lifecycle() {
    security_and_lifecycle(true).await;
}
#[tokio::test]
async fn reef_identity_consumer_lexical_scale() {
    lexical_scale(false).await;
}
#[tokio::test]
async fn reef_identity_tenant_lexical_scale() {
    lexical_scale(true).await;
}
