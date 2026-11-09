//! Deferred resolution of ambiguous entity mentions
//! (`coralswarm_backend::mention_sweep`) on real local Postgres: the org
//! tenant plane as `coralswarm_tenant_app` (system scope) and as the table
//! owner (production's `neondb_owner` shape), and the consumer plane as
//! `coralswarm_app`.
//!
//! `RELINFER_TEST_DATABASE_URL` names any database on a local server; every
//! test creates and drops its own databases. With `REQUIRE_PG_SUITE` set a
//! missing URL fails instead of skipping.
mod support;

use std::str::FromStr;

use coralswarm_backend::{
    mention_sweep::{self, Counts, Run, Write},
    store::PostgresStore,
    tenancy::scope::{begin_account_scoped, begin_tenant_system_scoped_without_account},
};
use serde_json::{json, Value};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    PgPool,
};
use uuid::Uuid;

fn base() -> Option<PgConnectOptions> {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        assert!(std::env::var_os("REQUIRE_PG_SUITE").is_none(), "RELINFER_TEST_DATABASE_URL required");
        eprintln!("skipping mention sweep test: set RELINFER_TEST_DATABASE_URL");
        return None;
    };
    let opts = PgConnectOptions::from_str(&url).unwrap();
    assert!(matches!(opts.get_host(), "127.0.0.1" | "localhost" | "::1"), "test database must be local");
    Some(opts)
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Role {
    App,
    Owner,
}

struct Plane {
    org: bool,
    owner: PgPool,
    _db: coralswarm_backend::test_db::TestDatabase,
    app: PgPool,
    account: Uuid,
    graph: Uuid,
}

impl Plane {
    async fn tenant(opts: &PgConnectOptions) -> Self {
        let admin = PgPoolOptions::new().max_connections(1).connect_with(opts.clone()).await.unwrap();
        let name = coralswarm_backend::test_db::TestDatabase::new(opts, "managed_atoms");
        sqlx::query(&format!("CREATE DATABASE {name}")).execute(&admin).await.unwrap();
        admin.close().await;
        let owner = PgPoolOptions::new().max_connections(4).connect_with(opts.clone().database(&name)).await.unwrap();
        sqlx::migrate!("./migrations_tenant").run(&owner).await.unwrap();
        support::tenant_role::set_app_role_password_with_retry(&owner).await;
        // Production runs the identity contract's phase two (no legacy title index).
        sqlx::query("DROP INDEX IF EXISTS nodes_graph_type_lower_title_unique").execute(&owner).await.unwrap();
        let app = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(
                opts.clone()
                    .database(&name)
                    .username("coralswarm_tenant_app")
                    .password(support::tenant_role::APP_ROLE_TEST_PASSWORD),
            )
            .await
            .unwrap();
        let account = Uuid::new_v4();
        let graph = sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'Sweep','member',$2) RETURNING id")
            .bind(account)
            .bind(Uuid::new_v4())
            .fetch_one(&owner)
            .await
            .unwrap();
        Self { org: true, owner, _db: name, app, account, graph }
    }

    async fn consumer(opts: &PgConnectOptions) -> Self {
        let (name, owner) = support::managed_extraction::isolated_database(opts).await;
        support::app_role::set_app_role_password_with_retry(&owner).await;
        // Production runs the identity contract's phase two (no legacy title index).
        sqlx::query("DROP INDEX IF EXISTS nodes_graph_type_lower_title_unique").execute(&owner).await.unwrap();
        let app = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(
                owner
                    .connect_options()
                    .as_ref()
                    .clone()
                    .username("coralswarm_app")
                    .password(support::app_role::APP_ROLE_TEST_PASSWORD),
            )
            .await
            .unwrap();
        let suffix = Uuid::new_v4();
        let user: Uuid = sqlx::query_scalar("INSERT INTO users(email,clerk_user_id) VALUES($1,$2) RETURNING id")
            .bind(format!("mention-sweep-{suffix}@test.local"))
            .bind(format!("mention-sweep-{suffix}"))
            .fetch_one(&owner)
            .await
            .unwrap();
        let account: Uuid = sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES($1) RETURNING id")
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
        let graph: Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id,name) VALUES($1,'Sweep') RETURNING id")
            .bind(account)
            .fetch_one(&owner)
            .await
            .unwrap();
        Self { org: false, owner, _db: name, app, account, graph }
    }

    async fn done(self) {
        self.app.close().await;
        support::managed_extraction::drop_database(&self.owner).await;
    }

    async fn sweep(&self, role: Role, write: Write) -> Counts {
        let pool = if role == Role::Owner { self.owner.clone() } else { self.app.clone() };
        let store = PostgresStore::new(pool.clone());
        let (org, account) = (self.org, self.account);
        let begin = || {
            let pool = pool.clone();
            async move {
                match (role, org) {
                    (Role::Owner, _) => Ok(pool.begin().await?),
                    (Role::App, true) => begin_tenant_system_scoped_without_account(&pool).await,
                    (Role::App, false) => begin_account_scoped(&pool, account).await,
                }
            }
        };
        let (counts, failed) = mention_sweep::sweep_graph(&store, begin, self.graph, write, None, &mut Run::default())
            .await
            .unwrap();
        assert!(failed.is_empty(), "{failed:?}");
        counts
    }

    /// A reef; `corals` sets the count the door reads as "consequential".
    async fn reef(&self, title: &str, entity_type: &str, corals: i32) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO nodes(graph_id,type,title,body,entity_type,coral_count,identity_scope) VALUES($1,'entity',$2,$3,$4,$5,gen_random_uuid()) RETURNING node_id",
        )
        .bind(self.graph)
        .bind(title)
        .bind(json!({"entity_type": entity_type}))
        .bind(entity_type)
        .bind(corals)
        .fetch_one(&self.owner)
        .await
        .unwrap()
    }

    /// One permitted source version holding one v2 atom whose mentions the
    /// binder left ambiguous.
    async fn atom(&self, mentions: &[(&str, &str)]) -> Uuid {
        let o = &self.owner;
        let id = Uuid::new_v4().to_string();
        let origin = json!({"service_id":"slack","transport":"connector","content_kind":"message"});
        let item: Uuid = sqlx::query_scalar(
            "INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,origin,access_state) \
             VALUES($1,'slack','composio:slack',$2,$3,'permitted') RETURNING source_item_id",
        )
        .bind(self.graph)
        .bind(&id)
        .bind(&origin)
        .fetch_one(o)
        .await
        .unwrap();
        let version: Uuid = sqlx::query_scalar(
            "INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,raw_inline,\
                extraction_version,extraction_status,origin,captured_at,occurred_at) \
             VALUES($1,$2,$3,'slack','composio:slack','text','source-atoms-v2','done',$4,now(),now()) RETURNING version_id",
        )
        .bind(self.graph)
        .bind(item)
        .bind(&id)
        .bind(&origin)
        .fetch_one(o)
        .await
        .unwrap();
        let entities: Vec<Value> = mentions
            .iter()
            .map(|(m, t)| json!({"mention": m, "type": t, "status": "unresolved", "reason": "ambiguous", "how": "ladder"}))
            .collect();
        sqlx::query_scalar(
            "INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model,entities) \
             VALUES($1,$2,0,'fact','A statement.','source-atoms-v2','fake-model',$3) RETURNING semantic_atom_id",
        )
        .bind(self.graph)
        .bind(version)
        .bind(json!(entities))
        .fetch_one(o)
        .await
        .unwrap()
    }

    async fn entities(&self, atom: Uuid) -> Vec<Value> {
        let v: Value = sqlx::query_scalar("SELECT entities FROM semantic_atoms WHERE semantic_atom_id=$1")
            .bind(atom)
            .fetch_one(&self.owner)
            .await
            .unwrap();
        v.as_array().unwrap().clone()
    }

    async fn links(&self, atom: Uuid) -> Vec<Uuid> {
        sqlx::query_scalar("SELECT node_id FROM node_semantic_atoms WHERE semantic_atom_id=$1 ORDER BY node_id")
            .bind(atom)
            .fetch_all(&self.owner)
            .await
            .unwrap()
    }

    async fn reefs_titled(&self, title: &str) -> Vec<Uuid> {
        sqlx::query_scalar("SELECT node_id FROM nodes WHERE graph_id=$1 AND lower(title)=lower($2) ORDER BY created_at, node_id")
            .bind(self.graph)
            .bind(title)
            .fetch_all(&self.owner)
            .await
            .unwrap()
    }

    async fn writes(&self) -> (i64, i64, i64) {
        sqlx::query_as(
            "SELECT (SELECT count(*) FROM nodes WHERE graph_id=$1), (SELECT count(*) FROM node_semantic_atoms WHERE graph_id=$1), \
                    (SELECT count(*) FROM op_log WHERE graph_id=$1)",
        )
        .bind(self.graph)
        .fetch_one(&self.owner)
        .await
        .unwrap()
    }
}

fn status(e: &Value) -> (&str, Option<&str>, Option<&str>) {
    (e["status"].as_str().unwrap(), e["reason"].as_str(), e["how"].as_str())
}

/// A mention left ambiguous next to a broader consequential reef binds to
/// the reef that now carries its exact title, not to the near match; a
/// mention that is only a longer form of that reef ("Atlas rework") stays
/// unresolved; a mention no reef relates to mints one reef for its title,
/// shared by every later mention of it; and a dry run writes nothing.
async fn resolves_to_the_right_reef(p: &Plane, role: Role) {
    let atlas = p.reef("Atlas", "project", 40).await;
    let redesign = p.reef("Atlas scale redesign", "project", 1).await;
    let first = p.atom(&[("Atlas scale redesign", "project"), ("Hermes rework", "project")]).await;
    // A longer form of the consequential "Atlas": a rival, never minted (the
    // v3.4.3 gate's duplicate mints).
    let variant = p.atom(&[("Atlas rework", "project")]).await;
    let second = p.atom(&[("Hermes rework", "project")]).await;

    let before = p.writes().await;
    let dry = p.sweep(role, Write::DryRun).await;
    // #1516: the non-person exact title binds in the door's ladder (step 3a),
    // the planned second "Hermes rework" by the sweep's exact-title rule.
    assert_eq!((dry.linked_ladder, dry.linked_exact_title, dry.minted, dry.kept_competing), (1, 1, 1, 1), "{dry:?}");
    assert_eq!(p.writes().await, before, "a dry run writes nothing");
    assert_eq!(status(&p.entities(first).await[0]), ("unresolved", Some("ambiguous"), Some("ladder")));

    let applied = p.sweep(role, Write::Apply).await;
    // Applied, the first "Hermes rework" mint is real, so the second mention
    // binds it in the ladder too (in the dry run it is only planned).
    assert_eq!((applied.linked_ladder, applied.linked_exact_title, applied.minted, applied.atoms_changed, applied.kept_competing), (2, 0, 1, 2, 1), "{applied:?}");
    assert_eq!(status(&p.entities(variant).await[0]), ("unresolved", Some("ambiguous"), Some("ladder")));
    assert!(p.reefs_titled("Atlas rework").await.is_empty(), "no duplicate of Atlas");
    let rework = p.reefs_titled("Hermes rework").await;
    assert_eq!(rework.len(), 1, "one reef per title, however many mentions");
    let mut expected = vec![redesign, rework[0]];
    expected.sort();
    assert_eq!(p.links(first).await, expected);
    assert_eq!(p.links(second).await, vec![rework[0]]);
    assert!(!p.links(first).await.contains(&atlas), "never the near match");
    let e = p.entities(first).await;
    assert_eq!(status(&e[0]), ("linked", None, Some("ladder")));
    assert_eq!(e[0]["swept"], mention_sweep::RULE);
    let later = p.entities(second).await;
    let mut rework_how = vec![status(&e[1]), status(&later[0])];
    rework_how.sort();
    assert_eq!(rework_how, vec![("linked", None, Some("ladder")), ("linked", None, Some("minted"))], "the first mention mints, the later one binds");
    let born: Value = sqlx::query_scalar("SELECT body->'born'->'worker' FROM nodes WHERE node_id=$1")
        .bind(rework[0])
        .fetch_one(&p.owner)
        .await
        .unwrap();
    assert_eq!(born, json!(mention_sweep::WORKER));
}

/// Mentions with a real competing candidate stay unresolved: a partial name
/// of a consequential person, and a project name that a consequential reef
/// extends. Nothing is minted or linked for them.
async fn genuinely_ambiguous_stays_unresolved(p: &Plane, role: Role) {
    p.reef("Bea Park", "person", 40).await;
    p.reef("Atlas redesign", "project", 40).await;
    let atom = p.atom(&[("Bea", "person"), ("Atlas", "project")]).await;
    let before = p.writes().await;
    let out = p.sweep(role, Write::Apply).await;
    assert_eq!((out.mentions, out.kept_competing, out.linked(), out.atoms_changed), (2, 2, 0, 0), "{out:?}");
    assert_eq!(p.writes().await, before);
    for e in p.entities(atom).await {
        assert_eq!(status(&e), ("unresolved", Some("ambiguous"), Some("ladder")));
    }
}

/// Two same-title reefs whose emails conflict are two people: the sweep
/// neither picks one nor mints a third copy.
async fn conflicting_identifiers_never_merge(p: &Plane, role: Role) {
    let a = p.reef("Dana Wu", "person", 1).await;
    let b = p.reef("Dana Wu", "person", 1).await;
    for (node, email) in [(a, "dana@one.example"), (b, "dana@two.example")] {
        sqlx::query("INSERT INTO reef_identifiers(graph_id,node_id,kind,value_norm,source) VALUES($1,$2,'email',$3,'test')")
            .bind(p.graph)
            .bind(node)
            .bind(email)
            .execute(&p.owner)
            .await
            .unwrap();
    }
    let atom = p.atom(&[("Dana Wu", "person")]).await;
    let out = p.sweep(role, Write::Apply).await;
    assert_eq!((out.kept_same_title_distinct, out.linked()), (1, 0), "{out:?}");
    assert_eq!(p.reefs_titled("Dana Wu").await.len(), 2, "no third copy");
    assert!(p.links(atom).await.is_empty());
    let merges: i64 = sqlx::query_scalar("SELECT count(*) FROM node_merges").fetch_one(&p.owner).await.unwrap();
    assert_eq!(merges, 0);
}

/// A second run over a swept graph decides the same and writes nothing.
async fn re_runs_are_idempotent(p: &Plane, role: Role) {
    p.reef("Atlas", "project", 40).await;
    p.reef("Bea Park", "person", 40).await;
    p.atom(&[("Hermes rework", "project"), ("Bea", "person")]).await;
    let first = p.sweep(role, Write::Apply).await;
    assert_eq!((first.minted, first.kept_competing), (1, 1), "{first:?}");
    let after = p.writes().await;
    let again = p.sweep(role, Write::Apply).await;
    assert_eq!((again.mentions, again.kept_competing, again.linked(), again.atoms_changed), (1, 1, 0, 0), "{again:?}");
    assert_eq!(p.writes().await, after, "no new reef, link or op");
}

/// The repair for the sweep's duplicate mints merges only the same code
/// entity under a namespace- or path-qualified name
/// (`jobs::queue::could_not_do` into `queue::could_not_do`), undoably
/// (`node_merges`), and its corals follow. The prod dry run's wrong merges
/// (a specific thing into its generic parent: "Google Tasks" into "Google")
/// are left alone, as are a mint two reefs qualify for, an unrelated mint
/// and a person. Dry run by default; a re-run finds nothing.
async fn repairs_duplicate_mints(p: &Plane, _role: Role) {
    let keep = p.reef("queue::could_not_do", "topic", 40).await;
    for (title, entity_type) in [("Google", "project"), ("MobileNav", "project"), ("LLM", "topic"), ("email", "topic"),
        ("Graph", "topic"), ("OAuth", "topic"), ("backend/src/x.rs", "project"), ("web/src/x.rs", "project"),
        ("README.md", "project"), ("ocean_health.rs", "project"), ("web/src/ocean_health.rs", "project")] {
        p.reef(title, entity_type, 40).await;
    }
    let mint = |title: &'static str, entity_type: &'static str| {
        let (owner, graph) = (p.owner.clone(), p.graph);
        async move {
            sqlx::query_scalar::<_, Uuid>("INSERT INTO nodes(graph_id,type,title,body,entity_type,identity_scope) VALUES($1,'entity',$2,$3,$4,gen_random_uuid()) RETURNING node_id")
                .bind(graph).bind(title)
                .bind(json!({"entity_type": entity_type, "born": {"worker": mention_sweep::WORKER}, "provisional": true}))
                .bind(entity_type).fetch_one(&owner).await.unwrap()
        }
    };
    let dup = mint("jobs::queue::could_not_do", "topic").await;
    let mut untouched = Vec::new();
    for (title, entity_type) in [("Google Tasks", "project"), ("MobileNav.tsx", "project"), ("LLM call", "topic"),
        ("from_email_name", "topic"), ("pinned graph ids", "topic"), ("OAuth writer", "topic"), ("src/x.rs", "project"),
        ("Hermes rework", "project"), ("Bea", "person"),
        // A ubiquitous basename is never "the" bare file; a bare name two
        // paths in the graph end with is ambiguous.
        ("widgets/README.md", "project"), ("backend/src/ocean_health.rs", "project")] {
        untouched.push(mint(title, entity_type).await);
    }
    p.reef("Bea Park", "person", 40).await;
    let atom = p.atom(&[("jobs::queue::could_not_do", "topic")]).await;
    sqlx::query("INSERT INTO node_semantic_atoms(node_id,semantic_atom_id,graph_id) VALUES($1,$2,$3)")
        .bind(dup).bind(atom).bind(p.graph).execute(&p.owner).await.unwrap();

    let before = p.writes().await;
    let dry = mention_sweep::repair_minted_duplicates(&p.owner, Some(p.graph), (None, None), Write::DryRun).await.unwrap();
    assert_eq!(dry.pairs, vec![mention_sweep::DuplicateMint { graph_id: p.graph, dup, keep }], "only the qualified code name: {dry:?}");
    assert_eq!((dry.examined, dry.no_match, dry.ambiguous), (11, 8, 2), "people are never examined; src/x.rs and ocean_health.rs have two qualified forms: {dry:?}");
    assert_eq!(p.writes().await, before, "a dry run writes nothing");

    let applied = mention_sweep::repair_minted_duplicates(&p.owner, Some(p.graph), (None, None), Write::Apply).await.unwrap();
    assert_eq!((applied.pairs.len(), applied.failed.len()), (1, 0), "{applied:?}");
    assert!(p.reefs_titled("jobs::queue::could_not_do").await.is_empty(), "the duplicate is merged away");
    assert_eq!(p.links(atom).await, vec![keep], "its coral follows to the survivor");
    let undo: i64 = sqlx::query_scalar("SELECT count(*) FROM node_merges WHERE keep_id=$1 AND dup_id=$2 AND undone_at IS NULL")
        .bind(keep).bind(dup).fetch_one(&p.owner).await.unwrap();
    assert_eq!(undo, 1, "undoable through node_merges");
    for left in untouched {
        let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM nodes WHERE node_id=$1)").bind(left).fetch_one(&p.owner).await.unwrap();
        assert!(exists, "{left} is left alone");
    }
    let again = mention_sweep::repair_minted_duplicates(&p.owner, Some(p.graph), (None, None), Write::Apply).await.unwrap();
    assert!(again.pairs.is_empty(), "a re-run finds nothing: {again:?}");
}

macro_rules! planes {
    ($($name:ident: $plane:ident => $run:ident($($arg:expr),*);)*) => {$(
        #[tokio::test]
        async fn $name() {
            let Some(opts) = base() else { return };
            let p = Plane::$plane(&opts).await;
            $run(&p $(, $arg)*).await;
            p.done().await;
        }
    )*};
}

planes! {
    tenant_app_resolves_to_the_right_reef: tenant => resolves_to_the_right_reef(Role::App);
    tenant_owner_resolves_to_the_right_reef: tenant => resolves_to_the_right_reef(Role::Owner);
    consumer_app_resolves_to_the_right_reef: consumer => resolves_to_the_right_reef(Role::App);
    tenant_app_genuinely_ambiguous_stays_unresolved: tenant => genuinely_ambiguous_stays_unresolved(Role::App);
    tenant_owner_genuinely_ambiguous_stays_unresolved: tenant => genuinely_ambiguous_stays_unresolved(Role::Owner);
    consumer_app_genuinely_ambiguous_stays_unresolved: consumer => genuinely_ambiguous_stays_unresolved(Role::App);
    tenant_app_conflicting_identifiers_never_merge: tenant => conflicting_identifiers_never_merge(Role::App);
    tenant_owner_conflicting_identifiers_never_merge: tenant => conflicting_identifiers_never_merge(Role::Owner);
    consumer_app_conflicting_identifiers_never_merge: consumer => conflicting_identifiers_never_merge(Role::App);
    tenant_app_re_runs_are_idempotent: tenant => re_runs_are_idempotent(Role::App);
    tenant_owner_re_runs_are_idempotent: tenant => re_runs_are_idempotent(Role::Owner);
    consumer_app_re_runs_are_idempotent: consumer => re_runs_are_idempotent(Role::App);
    tenant_owner_repairs_duplicate_mints: tenant => repairs_duplicate_mints(Role::Owner);
    consumer_owner_repairs_duplicate_mints: consumer => repairs_duplicate_mints(Role::Owner);
}
