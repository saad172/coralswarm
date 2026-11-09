//! Fact supersession over extraction v2's temporal facts (source-agnostic
//! plan PR 9), on real local Postgres: the org tenant plane as both
//! `coralswarm_tenant_app` (system scope) and the table owner (production's
//! `neondb_owner` shape), and the consumer plane as `coralswarm_app`. No
//! network model: the extraction and judge calls use scripted providers
//! through the real managed gate.
//!
//! `RELINFER_TEST_DATABASE_URL` names any database on a local server; every
//! test creates and drops its own databases. With `REQUIRE_PG_SUITE` set a
//! missing URL fails instead of skipping.
mod support;

use std::{
    collections::BTreeSet,
    str::FromStr,
    sync::{Arc, Mutex},
};

use chrono::{DateTime, Utc};
use coralswarm_backend::{
    fact_supersession::{self, judge, BackfillArgs, Mode, Report},
    jobs::queue::JobRow,
    library_read::{self, LibraryScope},
    llm::{CompletionRequest, CompletionResponse, LlmError, LlmProvider, StreamEvent},
    source_evidence::{self, CaptureSource},
    store::{PostgresStore, Store},
    tenancy::{
        extractor::Tenant,
        runtime::TenancyDecision,
        scope::{begin_account_scoped, begin_tenant_system_scoped_without_account},
        TenantId,
    },
    workers::{source_atoms, tenant_harness::WorkerTenant},
    UserContext,
};
use serde_json::{json, Value};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    PgPool, Postgres, Transaction,
};
use uuid::Uuid;

/// Process env (mode and enrichment switches) is shared by every test in
/// this binary; each test holds this for its whole run.
static ENV: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct EnvGuard(Vec<(&'static str, Option<std::ffi::OsString>)>);
impl EnvGuard {
    fn set(pairs: &[(&'static str, Option<&str>)]) -> Self {
        let saved = pairs.iter().map(|(k, _)| (*k, std::env::var_os(k))).collect();
        for (k, v) in pairs {
            match v {
                Some(v) => std::env::set_var(k, v),
                None => std::env::remove_var(k),
            }
        }
        Self(saved)
    }
}
impl Drop for EnvGuard {
    fn drop(&mut self) {
        for (k, v) in self.0.drain(..) {
            match v {
                Some(v) => std::env::set_var(k, v),
                None => std::env::remove_var(k),
            }
        }
    }
}
fn mode(m: &str) -> EnvGuard {
    EnvGuard::set(&[(fact_supersession::MODE_ENV, Some(m))])
}

fn base() -> Option<PgConnectOptions> {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        assert!(std::env::var_os("REQUIRE_PG_SUITE").is_none(), "RELINFER_TEST_DATABASE_URL required");
        eprintln!("skipping fact supersession test: set RELINFER_TEST_DATABASE_URL");
        return None;
    };
    let opts = PgConnectOptions::from_str(&url).unwrap();
    assert!(matches!(opts.get_host(), "127.0.0.1" | "localhost" | "::1"), "test database must be local");
    Some(opts)
}

fn at(day: &str) -> DateTime<Utc> {
    format!("{day}T00:00:00Z").parse().unwrap()
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Role {
    /// The low-privilege app role through its scoped transaction.
    App,
    /// The table owner (production tenant DSN), plain transaction.
    Owner,
}

struct Plane {
    org: bool,
    owner: PgPool,
    _db: coralswarm_backend::test_db::TestDatabase,
    app: PgPool,
    account: Uuid,
    user: Uuid,
    graph: Uuid,
}

struct Src {
    item: Uuid,
    version: Uuid,
    atom: Uuid,
}

impl Plane {
    /// An org tenant database built from `migrations_tenant`.
    async fn tenant(opts: &PgConnectOptions) -> Self {
        let admin = PgPoolOptions::new().max_connections(1).connect_with(opts.clone()).await.unwrap();
        let name = coralswarm_backend::test_db::TestDatabase::new(opts, "managed_atoms");
        sqlx::query(&format!("CREATE DATABASE {name}")).execute(&admin).await.unwrap();
        admin.close().await;
        let owner = PgPoolOptions::new().max_connections(4).connect_with(opts.clone().database(&name)).await.unwrap();
        sqlx::migrate!("./migrations_tenant").run(&owner).await.unwrap();
        support::tenant_role::set_app_role_password_with_retry(&owner).await;
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
        let (user, account) = (Uuid::new_v4(), Uuid::new_v4());
        let graph: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'Supersession','member',$2) RETURNING id",
        )
        .bind(account)
        .bind(user)
        .fetch_one(&owner)
        .await
        .unwrap();
        Self { org: true, owner, _db: name, app, account, user, graph }
    }

    /// The consumer plane (control migrations), funded so managed calls run.
    async fn consumer(opts: &PgConnectOptions) -> Self {
        let (name, owner) = support::managed_extraction::isolated_database(opts).await;
        support::app_role::set_app_role_password_with_retry(&owner).await;
        let app = PgPoolOptions::new()
            .max_connections(6)
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
            .bind(format!("fact-supersession-{suffix}@test.local"))
            .bind(format!("fact-supersession-{suffix}"))
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
        let mut financial = begin_account_scoped(&owner, account).await.unwrap();
        coralswarm_backend::billing::renewal::lock_period_rotation(&mut financial, account).await.unwrap();
        sqlx::query("INSERT INTO credit_balances(account_id,balance) VALUES($1,100)")
            .bind(account)
            .execute(&mut *financial)
            .await
            .unwrap();
        financial.commit().await.unwrap();
        // The managed lifecycle contract production carries (see
        // `support::managed_extraction::own_version`).
        let mut connection = owner.acquire().await.unwrap();
        sqlx::query("SET app.identity_serving_contract='composite-v1'").execute(&mut *connection).await.unwrap();
        sqlx::raw_sql(include_str!("../schema/identity-contract.sql")).execute(&mut *connection).await.unwrap();
        drop(connection);
        let graph: Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id,name) VALUES($1,'Supersession') RETURNING id")
            .bind(account)
            .fetch_one(&owner)
            .await
            .unwrap();
        Self { org: false, owner, _db: name, app, account, user, graph }
    }

    async fn done(self) {
        self.app.close().await;
        support::managed_extraction::drop_database(&self.owner).await;
    }

    async fn begin(&self, role: Role) -> Transaction<'static, Postgres> {
        match (role, self.org) {
            (Role::Owner, _) => self.owner.begin().await.unwrap(),
            (Role::App, true) => begin_tenant_system_scoped_without_account(&self.app).await.unwrap(),
            (Role::App, false) => begin_account_scoped(&self.app, self.account).await.unwrap(),
        }
    }

    async fn node(&self, title: &str) -> Uuid {
        sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title,body) VALUES($1,'entity',$2,'{}') RETURNING node_id")
            .bind(self.graph)
            .bind(title)
            .fetch_one(&self.owner)
            .await
            .unwrap()
    }

    /// One retained source version holding one v2 atom whose passage is the
    /// whole text, linked to `links`.
    async fn source(&self, id: &str, text: &str, captured: &str, atom_validity: (Option<&str>, Option<&str>, Option<&str>), links: &[Uuid]) -> Src {
        self.source_as("linear_issue", id, text, captured, atom_validity, links).await
    }

    async fn source_as(&self, source_type: &str, id: &str, text: &str, captured: &str, atom_validity: (Option<&str>, Option<&str>, Option<&str>), links: &[Uuid]) -> Src {
        let o = &self.owner;
        let origin = json!({"service_id":"linear","transport":"connector","content_kind":"issue"});
        let item: Uuid = sqlx::query_scalar(
            "INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,origin) \
             VALUES($1,$4,'composio:linear',$2,$3) RETURNING source_item_id",
        )
        .bind(self.graph)
        .bind(id)
        .bind(&origin)
        .bind(source_type)
        .fetch_one(o)
        .await
        .unwrap();
        let version: Uuid = sqlx::query_scalar(
            "INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,raw_inline,\
                extraction_version,extraction_status,origin,captured_at,occurred_at) \
             VALUES($1,$2,$3,$7,'composio:linear',$4,'source-atoms-v2','done',$5,$6,$6) RETURNING version_id",
        )
        .bind(self.graph)
        .bind(item)
        .bind(id)
        .bind(text)
        .bind(&origin)
        .bind(at(captured))
        .bind(source_type)
        .fetch_one(o)
        .await
        .unwrap();
        sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
            .bind(item)
            .bind(version)
            .execute(o)
            .await
            .unwrap();
        let (from, until, quote) = atom_validity;
        let atom: Uuid = sqlx::query_scalar(
            "INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model,valid_from,valid_until,validity_quote) \
             VALUES($1,$2,0,'fact',$3,'source-atoms-v2','fake-model',$4,$5,$6) RETURNING semantic_atom_id",
        )
        .bind(self.graph)
        .bind(version)
        .bind(text)
        .bind(from.map(at))
        .bind(until.map(at))
        .bind(quote)
        .fetch_one(o)
        .await
        .unwrap();
        sqlx::query("INSERT INTO semantic_atom_passages(graph_id,source_version_id,semantic_atom_id,start_char,end_char,quote) VALUES($1,$2,$3,0,$4,$5)")
            .bind(self.graph)
            .bind(version)
            .bind(atom)
            .bind(text.chars().count() as i32)
            .bind(text)
            .execute(o)
            .await
            .unwrap();
        for node in links {
            sqlx::query("INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)")
                .bind(self.graph)
                .bind(node)
                .bind(atom)
                .execute(o)
                .await
                .unwrap();
        }
        Src { item, version, atom }
    }

    /// A stated v2 relation, as `source_atoms_v2::write_relations_tx` writes it.
    async fn relation(&self, src: Uuid, predicate: &str, dst: Uuid, atom: Uuid, from: Option<&str>, until: Option<&str>, quote: Option<&str>) {
        let provenance = json!({
            "extraction_version": "source-atoms-v2",
            "semantic_atom_ids": [atom.to_string()],
            "assertions": [{"semantic_atom_id": atom.to_string(), "valid_from": from.map(at), "valid_until": until.map(at), "validity_quote": quote}],
        });
        sqlx::query(
            "INSERT INTO relationship_edge(graph_id,src_node_id,predicate,dst_node_id,origin,status,confidence,provenance,valid_from,valid_until) \
             VALUES($1,$2,$3,$4,'stated','confirmed',0.9,$5,$6,$7) \
             ON CONFLICT (graph_id,src_node_id,predicate,dst_node_id) DO UPDATE SET provenance=jsonb_set(relationship_edge.provenance,'{assertions}', \
               (relationship_edge.provenance->'assertions')||(EXCLUDED.provenance->'assertions'))",
        )
        .bind(self.graph)
        .bind(src)
        .bind(predicate)
        .bind(dst)
        .bind(provenance)
        .bind(from.map(at))
        .bind(until.map(at))
        .execute(&self.owner)
        .await
        .unwrap();
    }

    /// Mark `src`'s atom as a v3 atom carrying `changes`, as
    /// `source_atoms::v3::change_json` writes them.
    async fn stated(&self, src: &Src, changes: Value) {
        sqlx::query("UPDATE source_versions SET extraction_version='source-atoms-v3' WHERE version_id=$1")
            .bind(src.version)
            .execute(&self.owner)
            .await
            .unwrap();
        sqlx::query("UPDATE semantic_atoms SET extraction_version='source-atoms-v3',stated_changes=$2 WHERE semantic_atom_id=$1")
            .bind(src.atom)
            .bind(changes)
            .execute(&self.owner)
            .await
            .unwrap();
    }

    async fn process(&self, version: Uuid, m: Mode, role: Role) -> Report {
        let mut tx = self.begin(role).await;
        let report = fact_supersession::process_version_tx(&mut tx, self.graph, version, m, true).await.unwrap();
        tx.commit().await.unwrap();
        report
    }

    async fn records(&self) -> Vec<(String, String, Option<String>, Option<DateTime<Utc>>, Option<DateTime<Utc>>, Uuid, Option<Uuid>, Uuid)> {
        sqlx::query_as(
            "SELECT state_key,operation,value,valid_from,valid_until,semantic_atom_id,predecessor_event_id,event_id \
               FROM temporal_evidence_records WHERE graph_id=$1 AND recording_method='temporal_extraction_v1' ORDER BY known_at,event_id",
        )
        .bind(self.graph)
        .fetch_all(&self.owner)
        .await
        .unwrap()
    }

    async fn edges(&self) -> Vec<(String, Uuid, Uuid, String, String, String)> {
        sqlx::query_as(
            "SELECT state_key,superseder_atom_id,superseded_atom_id,rule,status,kind FROM fact_supersession_edges \
              WHERE graph_id=$1 ORDER BY state_key,created_at,edge_id",
        )
        .bind(self.graph)
        .fetch_all(&self.owner)
        .await
        .unwrap()
    }

    async fn count(&self, sql: &str) -> i64 {
        sqlx::query_scalar(sql).bind(self.graph).fetch_one(&self.owner).await.unwrap()
    }
}

/// X owns Apollo until March; Z owns Apollo from April; W owns Apollo from
/// May, but a human marked Z and W as not a supersession pair.
struct Story {
    apollo: Uuid,
    x: Uuid,
    z: Uuid,
    w: Uuid,
    v1: Src,
    v2: Src,
    v3: Src,
}

async fn story(p: &Plane) -> Story {
    let apollo = p.node("Apollo").await;
    let x = p.node("Xavier Ortiz").await;
    let z = p.node("Zoe Park").await;
    let w = p.node("Wen Li").await;
    let v1 = p.source("own-1", "Xavier Ortiz owns Apollo until 2026-03-01.", "2026-02-01", (None, Some("2026-03-01"), Some("until 2026-03-01")), &[x, apollo]).await;
    p.relation(x, "responsible_for", apollo, v1.atom, None, Some("2026-03-01"), Some("until 2026-03-01")).await;
    // The materialized inverse is the same fact, not a second one.
    p.relation(apollo, "responsibility_of", x, v1.atom, None, Some("2026-03-01"), Some("until 2026-03-01")).await;
    let v2 = p.source("own-2", "Zoe Park owns Apollo from 2026-04-01.", "2026-04-02", (Some("2026-04-01"), None, Some("from 2026-04-01")), &[z, apollo]).await;
    p.relation(z, "responsible_for", apollo, v2.atom, Some("2026-04-01"), None, Some("from 2026-04-01")).await;
    let v3 = p.source("own-3", "Wen Li owns Apollo from 2026-05-01.", "2026-05-02", (Some("2026-05-01"), None, Some("from 2026-05-01")), &[w, apollo]).await;
    p.relation(w, "responsible_for", apollo, v3.atom, Some("2026-05-01"), None, Some("from 2026-05-01")).await;
    sqlx::query("INSERT INTO non_supersession_pair(node_a_id,node_b_id) VALUES($1,$2)")
        .bind(z.min(w))
        .bind(z.max(w))
        .execute(&p.owner)
        .await
        .unwrap();
    Story { apollo, x, z, w, v1, v2, v3 }
}

fn owner_edges(edges: &[(String, Uuid, Uuid, String, String, String)]) -> Vec<(Uuid, Uuid, &str, &str)> {
    edges
        .iter()
        .filter(|e| e.0 == "rel.owner")
        .map(|e| (e.1, e.2, e.3.as_str(), e.4.as_str()))
        .collect()
}

async fn deterministic_story(p: &Plane, role: Role) {
    let s = story(p).await;
    let _ = (s.x, s.z, s.w);
    for v in [&s.v1, &s.v2, &s.v3] {
        p.process(v.version, Mode::Live, role).await;
    }

    // Temporal evidence from v2 facts: an assert per fact on the subject that
    // holds the single-valued key (Apollo.rel.owner), the end-only validity
    // as a chained withdraw, and the decided replacements as transitions.
    let records = p.records().await;
    let owner: Vec<_> = records.iter().filter(|r| r.0 == "rel.owner").collect();
    let assert_x = owner.iter().find(|r| r.1 == "assert" && r.5 == s.v1.atom).expect("X assert");
    assert_eq!((assert_x.2.as_deref(), assert_x.3, assert_x.4), (Some("Xavier Ortiz"), None, None), "an end-only validity has no start");
    let withdraw = owner.iter().find(|r| r.1 == "withdraw").expect("withdraw");
    assert_eq!((withdraw.5, withdraw.6, withdraw.3, withdraw.2.as_deref()), (s.v1.atom, Some(assert_x.7), Some(at("2026-03-01")), None));
    let assert_z = owner.iter().find(|r| r.1 == "assert" && r.5 == s.v2.atom).expect("Z assert");
    assert_eq!((assert_z.2.as_deref(), assert_z.3), (Some("Zoe Park"), Some(at("2026-04-01"))));
    let mut transitions: Vec<_> = owner.iter().filter(|r| r.1 == "transition").map(|r| (r.5, r.6, r.2.as_deref(), r.3)).collect();
    transitions.sort_by_key(|t| t.3);
    assert_eq!(
        transitions,
        vec![
            (s.v2.atom, Some(assert_x.7), Some("Zoe Park"), Some(at("2026-04-01"))),
            (s.v3.atom, Some(assert_x.7), Some("Wen Li"), Some(at("2026-05-01"))),
        ],
        "each replacement chains to the replaced fact's assert; the blocked pair writes none"
    );
    assert_eq!(owner.iter().filter(|r| r.1 == "assert").count(), 3, "forward and inverse edges are one fact");

    // Deterministic decisions: explicit end, then the non-supersession pair.
    let edges = p.edges().await;
    let mut owned = owner_edges(&edges);
    owned.sort();
    let mut expected = vec![
        (s.v2.atom, s.v1.atom, "explicit_end", "active"),
        (s.v3.atom, s.v1.atom, "explicit_end", "active"),
        (s.v3.atom, s.v2.atom, "later_start", "blocked"),
    ];
    expected.sort();
    assert_eq!(owned, expected);
    assert!(edges.iter().all(|e| e.5 == "change"));
    // The atoms' own dated statements are the same pairs: no second
    // (judge) question for an atom pair a relation already decided.
    assert_eq!(edges.iter().filter(|e| e.0.starts_with("fact.")).count(), 0, "{edges:?}");

    // Idempotent per version: reprocessing writes nothing new.
    let before = (p.records().await.len(), p.edges().await.len());
    for v in [&s.v1, &s.v2, &s.v3] {
        let again = p.process(v.version, Mode::Live, role).await;
        assert_eq!((again.asserts, again.withdraws, again.transitions, again.active, again.proposed, again.blocked), (0, 0, 0, 0, 0, 0));
    }
    assert_eq!((p.records().await.len(), p.edges().await.len()), before);

    // Deleting a source retracts what its facts produced, and nothing else.
    sqlx::query("DELETE FROM source_items WHERE source_item_id=$1").bind(s.v2.item).execute(&p.owner).await.unwrap();
    let remaining = p.edges().await;
    let after: Vec<_> = owner_edges(&remaining).into_iter().map(|e| (e.0, e.1, e.3)).collect();
    assert_eq!(after, vec![(s.v3.atom, s.v1.atom, "active")]);
    let records = p.records().await;
    assert!(records.iter().all(|r| r.5 != s.v2.atom), "the deleted source's evidence is gone");
    assert_eq!(records.iter().filter(|r| r.1 == "transition").count(), 1);
    let _ = s.apollo;
}

/// Only single-valued state is ever paired. Ana's projects and teams change
/// with every shape a pair rule reads (a stated end, a later start, words
/// naming the old value), and none is a decision; Apollo's owner changing
/// is. Decisions proposed before the rule (a `works_on` pair) are pruned,
/// except one the judge already decided.
async fn only_single_valued_state_is_proposed(p: &Plane, role: Role) {
    let ana = p.node("Ana Ruiz").await;
    let apollo = p.node("Apollo").await;
    let hermes = p.node("Hermes").await;
    let core = p.node("Core team").await;
    let infra = p.node("Infra team").await;
    let x = p.node("Xavier Ortiz").await;
    let z = p.node("Zoe Park").await;
    let w1 = p.source("mv-1", "Ana Ruiz works on Apollo and is in the Core team until 2026-03-01.", "2026-02-01", (None, None, None), &[ana, apollo, core]).await;
    p.relation(ana, "works_on", apollo, w1.atom, None, Some("2026-03-01"), Some("until 2026-03-01")).await;
    p.relation(ana, "member_of", core, w1.atom, None, Some("2026-03-01"), Some("until 2026-03-01")).await;
    let w2 = p.source("mv-2", "Ana Ruiz moved from Apollo to Hermes and replaced Core team with Infra team from 2026-04-01.", "2026-04-02",
        (None, None, None), &[ana, hermes, infra]).await;
    p.relation(ana, "works_on", hermes, w2.atom, Some("2026-04-01"), None, Some("from 2026-04-01")).await;
    p.relation(ana, "member_of", infra, w2.atom, Some("2026-04-01"), None, Some("from 2026-04-01")).await;
    let o1 = p.source("sv-1", "Xavier Ortiz owns Apollo until 2026-03-01.", "2026-02-01", (None, None, None), &[x, apollo]).await;
    p.relation(x, "responsible_for", apollo, o1.atom, None, Some("2026-03-01"), Some("until 2026-03-01")).await;
    let o2 = p.source("sv-2", "Zoe Park owns Apollo from 2026-04-01.", "2026-04-02", (None, None, None), &[z, apollo]).await;
    p.relation(z, "responsible_for", apollo, o2.atom, Some("2026-04-01"), None, Some("from 2026-04-01")).await;
    for v in [&w1, &w2, &o1, &o2] {
        p.process(v.version, Mode::DryRun, role).await;
    }
    let decided: Vec<_> = p.edges().await.into_iter().map(|e| (e.0, e.1, e.2, e.3, e.4)).collect();
    assert_eq!(decided, vec![("rel.owner".to_string(), o2.atom, o1.atom, "explicit_end".to_string(), "proposed".to_string())]);
    let keys: BTreeSet<String> = p.records().await.into_iter().map(|r| r.0).collect();
    assert!(keys.contains("rel.works_on") && keys.contains("rel.member_of"), "many-valued validity is still evidence: {keys:?}");

    // What the old rule left behind: an undecided `works_on` pair, and one
    // the judge already rejected.
    let legacy = |status: &'static str, attempts: i16, superseder: Uuid, superseded: Uuid| {
        sqlx::query(
            "INSERT INTO fact_supersession_edges(graph_id,subject_node_id,state_key,superseder_atom_id,superseded_atom_id,\
                superseder_fact,superseded_fact,rule,status,judge_attempts) \
             VALUES($1,$2,'rel.works_on',$3,$4,md5($3::text),md5($4::text),'judge',$5,$6)",
        )
        .bind(p.graph)
        .bind(ana)
        .bind(superseder)
        .bind(superseded)
        .bind(status)
        .bind(attempts)
        .execute(&p.owner)
    };
    legacy("ambiguous", 0, w2.atom, w1.atom).await.unwrap();
    legacy("rejected", 1, w1.atom, w2.atom).await.unwrap();
    assert_eq!(fact_supersession::prune_ineligible(&p.owner, Some(p.graph), false).await.unwrap().pruned.values().sum::<u64>(), 1, "dry run counts");
    assert_eq!(p.edges().await.len(), 3, "dry run deletes nothing");
    assert_eq!(fact_supersession::prune_ineligible(&p.owner, Some(p.graph), true).await.unwrap().pruned.values().sum::<u64>(), 1);
    let left: Vec<_> = p.edges().await.into_iter().map(|e| (e.0, e.4)).collect();
    assert_eq!(left, vec![("rel.owner".into(), "proposed".into()), ("rel.works_on".into(), "rejected".into())]);
    assert_eq!(fact_supersession::prune_ineligible(&p.owner, Some(p.graph), true).await.unwrap().pruned.values().sum::<u64>(), 0, "idempotent");
}

/// What the independent review of production found, each a case the rules
/// must refuse: an owner change stated in a code file and in a test file
/// (fixtures, recorded and paired nowhere); a replacement whose two facts
/// share one observation time; a quote that names its own value as the
/// earlier one; a short label and a full name for one owner. The same owner
/// change from a real source is still proposed. Legacy rows of each kind
/// are pruned, by reason, and the judge never claims a code row.
async fn live_safety(p: &Plane, role: Role) {
    let apollo = p.node("Apollo").await;
    let x = p.node("Xavier Ortiz").await;
    let z = p.node("Zoe Park").await;
    let story = |prefix: &'static str, source_type: &'static str, x_id: &'static str, z_id: &'static str| async move {
        let a = p.source_as(source_type, x_id, "Xavier Ortiz owns Apollo until 2026-03-01.", "2026-02-01", (None, None, None), &[x, apollo]).await;
        p.relation(x, "responsible_for", apollo, a.atom, None, Some("2026-03-01"), Some("until 2026-03-01")).await;
        let b = p.source_as(source_type, z_id, "Zoe Park owns Apollo from 2026-04-01.", "2026-04-02", (None, None, None), &[z, apollo]).await;
        p.relation(z, "responsible_for", apollo, b.atom, Some("2026-04-01"), None, Some("from 2026-04-01")).await;
        let _ = prefix;
        (a, b)
    };
    let (ca, cb) = story("code", "github_code", "backend/src/owners.rs", "backend/src/owners_v2.rs").await;
    let (ta, tb) = story("test", "linear_issue", "backend/tests/owners_pg.rs", "web/app/owners.test.ts").await;
    for v in [&ca, &cb, &ta, &tb] {
        let r = p.process(v.version, Mode::Live, role).await;
        assert_eq!((r.facts, r.asserts, r.active), (0, 0, 0), "code and test files state nothing: {r:?}");
    }
    assert!(p.edges().await.is_empty() && p.records().await.is_empty());

    // R3 on a tie: one capture time, replacement words naming the old owner.
    let wen = p.node("Wen Li").await;
    let hermes = p.node("Hermes").await;
    let t1 = p.source("tie-1", "Wen Li owns Hermes.", "2026-05-01", (None, None, None), &[wen, hermes]).await;
    p.relation(wen, "responsible_for", hermes, t1.atom, None, None, None).await;
    let t2 = p.source("tie-2", "Zoe Park took over Hermes from Wen Li.", "2026-05-01", (None, None, None), &[z, hermes]).await;
    p.relation(z, "responsible_for", hermes, t2.atom, None, None, None).await;
    // A quote that puts its own value in the past is the old side.
    let orbit = p.node("Orbit").await;
    let r1 = p.source("rev-1", "Zoe Park owns Orbit.", "2026-05-01", (None, None, None), &[z, orbit]).await;
    p.relation(z, "responsible_for", orbit, r1.atom, None, None, None).await;
    let r2 = p.source("rev-2", "Orbit is now with Zoe Park; previously owned by Wen Li.", "2026-06-01", (None, None, None), &[wen, orbit]).await;
    p.relation(wen, "responsible_for", orbit, r2.atom, None, None, None).await;
    // One owner under a short label and a full name, and under an alias.
    let ana = p.node("Ana").await;
    let ana_full = p.node("Ana Ruiz").await;
    let atlas = p.node("Atlas").await;
    let a1 = p.source("ali-1", "Ana owns Atlas until 2026-03-01.", "2026-02-01", (None, None, None), &[ana, atlas]).await;
    p.relation(ana, "responsible_for", atlas, a1.atom, None, Some("2026-03-01"), Some("until 2026-03-01")).await;
    let a2 = p.source("ali-2", "Ana Ruiz owns Atlas from 2026-04-01.", "2026-04-02", (None, None, None), &[ana_full, atlas]).await;
    p.relation(ana_full, "responsible_for", atlas, a2.atom, Some("2026-04-01"), None, Some("from 2026-04-01")).await;
    let bo = p.node("Bo").await;
    let robert = p.node("Robert Chen").await;
    sqlx::query("UPDATE nodes SET body=jsonb_set(body,'{aliases}','[\"bo\"]') WHERE node_id=$1").bind(robert).execute(&p.owner).await.unwrap();
    let vega = p.node("Vega").await;
    let b1 = p.source("ali-3", "Bo owns Vega until 2026-03-01.", "2026-02-01", (None, None, None), &[bo, vega]).await;
    p.relation(bo, "responsible_for", vega, b1.atom, None, Some("2026-03-01"), Some("until 2026-03-01")).await;
    let b2 = p.source("ali-4", "Robert Chen owns Vega from 2026-04-01.", "2026-04-02", (None, None, None), &[robert, vega]).await;
    p.relation(robert, "responsible_for", vega, b2.atom, Some("2026-04-01"), None, Some("from 2026-04-01")).await;
    // The control: a real owner change.
    let (sa, sb) = story("stated", "linear_issue", "LIN-1", "LIN-2").await;
    for v in [&t1, &t2, &r1, &r2, &a1, &a2, &b1, &b2, &sa, &sb] {
        p.process(v.version, Mode::DryRun, role).await;
    }
    let decided: Vec<_> = p.edges().await.into_iter().map(|e| (e.1, e.2, e.3, e.4)).collect();
    assert_eq!(decided, vec![(sb.atom, sa.atom, "explicit_end".to_string(), "proposed".to_string())], "only the real change");

    // Rows the old rules wrote, one per reason, and a judged one that stays.
    let legacy = |rule: &'static str, status: &'static str, attempts: i16, er: &Src, ed: &Src, prov: Value| {
        let (er, ed) = (er.atom, ed.atom);
        async move {
            sqlx::query(
                "INSERT INTO fact_supersession_edges(graph_id,subject_node_id,state_key,superseder_atom_id,superseded_atom_id,\
                    superseder_fact,superseded_fact,rule,status,judge_attempts,provenance) \
                 VALUES($1,$2,'rel.owner',$3,$4,md5($3::text||$5),md5($4::text||$5),$5,$6,$7,$8)",
            )
            .bind(p.graph).bind(apollo).bind(er).bind(ed).bind(rule).bind(status).bind(attempts).bind(prov)
            .execute(&p.owner).await.unwrap();
        }
    };
    let side = |src: &Src, value: &str, node: Uuid, observed: &str| {
        json!({"semantic_atom_id": src.atom, "value": value, "value_node_id": node, "observed_at": at(observed)})
    };
    legacy("explicit_end", "proposed", 0, &cb, &ca, json!({"superseder": side(&cb, "Zoe Park", z, "2026-04-02"), "superseded": side(&ca, "Xavier Ortiz", x, "2026-02-01")})).await;
    legacy("judge", "ambiguous", 0, &tb, &ta, json!({"superseder": side(&tb, "Zoe Park", z, "2026-04-02"), "superseded": side(&ta, "Xavier Ortiz", x, "2026-02-01")})).await;
    legacy("replacement_language", "proposed", 0, &t2, &t1, json!({"superseder": side(&t2, "Zoe Park", z, "2026-05-01"), "superseded": side(&t1, "Wen Li", wen, "2026-05-01")})).await;
    legacy("later_start", "proposed", 0, &a2, &a1, json!({"superseder": side(&a2, "Ana Ruiz", ana_full, "2026-04-02"), "superseded": side(&a1, "Ana", ana, "2026-02-01")})).await;
    legacy("later_start", "proposed", 0, &b2, &b1, json!({"superseder": side(&b2, "Robert Chen", robert, "2026-04-02"), "superseded": side(&b1, "Bo", bo, "2026-02-01")})).await;
    let r2_passage: Uuid = sqlx::query_scalar("SELECT passage_id FROM semantic_atom_passages WHERE semantic_atom_id=$1").bind(r2.atom).fetch_one(&p.owner).await.unwrap();
    let mut reversed = side(&r2, "Wen Li", wen, "2026-06-01");
    reversed["passage_id"] = json!(r2_passage);
    legacy("replacement_language", "proposed", 0, &r2, &r1, json!({"superseder": reversed, "superseded": side(&r1, "Zoe Park", z, "2026-05-01")})).await;
    legacy("judge", "rejected", 1, &ca, &cb, json!({"superseder": side(&ca, "Xavier Ortiz", x, "2026-02-01"), "superseded": side(&cb, "Zoe Park", z, "2026-04-02")})).await;

    // The judge never claims a row whose facts come from code or tests.
    if !p.org {
        let model = Judge { answer: json!({"q0":{"choice":"supersede","confidence":0.92}}).to_string(), calls: Mutex::new(Vec::new()) };
        let worker = WorkerTenant { tenant: TenantId::Consumer, pool: p.app.clone(), store: Arc::new(PostgresStore::new(p.app.clone())) };
        let _g = EnvGuard::set(&[
            (fact_supersession::MODE_ENV, Some("live")),
            ("ENRICHMENT_PROCESSING_ENABLED", Some("true")),
            ("MANAGED_PROCESSING_DISABLED", None),
            ("MANAGED_DAILY_CAP_USD_ENRICHMENT", None),
        ]);
        let tick = judge::tick(&worker, &p.owner, &coralswarm_backend::llm::testing::GatedScript(&model)).await.unwrap();
        assert_eq!(tick.processed, 0);
        assert!(model.calls.lock().unwrap().is_empty(), "no judge call for a test-file pair");
        assert_eq!(p.count("SELECT count(*) FROM fact_supersession_edges WHERE graph_id=$1 AND status='ambiguous' AND judge_attempts=0").await, 1);
    }

    let dry = fact_supersession::prune_ineligible(&p.owner, Some(p.graph), false).await.unwrap();
    let pruned: Vec<(&str, u64)> = dry.pruned.iter().map(|(k, v)| (*k, *v)).collect();
    assert_eq!(pruned, vec![("captured_code", 2), ("reversed_replacement", 1), ("same_entity", 2), ("tied_observation", 1)]);
    let remaining: Vec<(String, String, u64)> = dry.remaining.into_iter().map(|((r, s), n)| (r, s, n)).collect();
    assert_eq!(remaining, vec![("explicit_end".into(), "proposed".into(), 1), ("judge".into(), "rejected".into(), 1)]);
    assert_eq!(p.edges().await.len(), 8, "dry run deletes nothing");
    let done = fact_supersession::prune_ineligible(&p.owner, Some(p.graph), true).await.unwrap();
    assert_eq!(done.pruned.values().sum::<u64>(), 6);
    assert_eq!(p.edges().await.len(), 2);
    assert!(fact_supersession::prune_ineligible(&p.owner, Some(p.graph), true).await.unwrap().pruned.is_empty(), "idempotent");
}

#[tokio::test]
async fn tenant_live_safety_as_app_role() {
    let _lock = ENV.lock().await;
    let Some(opts) = base() else { return };
    let p = Plane::tenant(&opts).await;
    live_safety(&p, Role::App).await;
    p.done().await;
}

#[tokio::test]
async fn tenant_live_safety_as_table_owner() {
    let _lock = ENV.lock().await;
    let Some(opts) = base() else { return };
    let p = Plane::tenant(&opts).await;
    live_safety(&p, Role::Owner).await;
    p.done().await;
}

#[tokio::test]
async fn consumer_live_safety_as_app_role() {
    let _lock = ENV.lock().await;
    let Some(opts) = base() else { return };
    let p = Plane::consumer(&opts).await;
    live_safety(&p, Role::App).await;
    p.done().await;
}

#[tokio::test]
async fn tenant_only_single_valued_state_is_proposed_as_app_role() {
    let _lock = ENV.lock().await;
    let Some(opts) = base() else { return };
    let p = Plane::tenant(&opts).await;
    only_single_valued_state_is_proposed(&p, Role::App).await;
    p.done().await;
}

#[tokio::test]
async fn tenant_only_single_valued_state_is_proposed_as_table_owner() {
    let _lock = ENV.lock().await;
    let Some(opts) = base() else { return };
    let p = Plane::tenant(&opts).await;
    only_single_valued_state_is_proposed(&p, Role::Owner).await;
    p.done().await;
}

#[tokio::test]
async fn consumer_only_single_valued_state_is_proposed_as_app_role() {
    let _lock = ENV.lock().await;
    let Some(opts) = base() else { return };
    let p = Plane::consumer(&opts).await;
    only_single_valued_state_is_proposed(&p, Role::App).await;
    p.done().await;
}

#[tokio::test]
async fn tenant_deterministic_supersession_chains_and_retracts_as_app_role() {
    let _lock = ENV.lock().await;
    let Some(opts) = base() else { return };
    let p = Plane::tenant(&opts).await;
    let role: String = sqlx::query_scalar("SELECT current_user::text").fetch_one(&p.app).await.unwrap();
    assert_eq!(role, "coralswarm_tenant_app");
    deterministic_story(&p, Role::App).await;
    p.done().await;
}

#[tokio::test]
async fn tenant_deterministic_supersession_chains_and_retracts_as_table_owner() {
    let _lock = ENV.lock().await;
    let Some(opts) = base() else { return };
    let p = Plane::tenant(&opts).await;
    let owns: bool = sqlx::query_scalar(
        "SELECT tableowner=current_user FROM pg_tables WHERE schemaname='public' AND tablename='fact_supersession_edges'",
    )
    .fetch_one(&p.owner)
    .await
    .unwrap();
    assert!(owns, "the production tenant DSN shape: the connecting role owns the table");
    deterministic_story(&p, Role::Owner).await;
    p.done().await;
}

#[tokio::test]
async fn consumer_deterministic_supersession_chains_and_retracts_as_app_role() {
    let _lock = ENV.lock().await;
    let Some(opts) = base() else { return };
    let p = Plane::consumer(&opts).await;
    deterministic_story(&p, Role::App).await;
    p.done().await;
}

/// Dry run writes evidence and `proposed` decisions only; the backfill in
/// live mode promotes them and writes the chains; a second pass writes
/// nothing; and the read side honors decisions only in live mode.
#[tokio::test]
async fn dry_run_then_live_backfill_is_idempotent_and_reads_prefer_current() {
    let _lock = ENV.lock().await;
    let Some(opts) = base() else { return };
    let p = Plane::consumer(&opts).await;
    let s = story(&p).await;
    let dry = BackfillArgs { execute: true, mode: Mode::DryRun, graph_id: Some(p.graph), after: None, limit: 100 };
    let preview = fact_supersession::backfill(&p.owner, &BackfillArgs { execute: false, ..dry.clone() }).await.unwrap();
    assert_eq!((preview.candidates, preview.report.versions, preview.report.proposed, preview.report.blocked), (3, 3, 2, 1));
    assert!(p.records().await.is_empty() && p.edges().await.is_empty(), "a dry-run preview writes nothing");

    let first = fact_supersession::backfill(&p.owner, &dry).await.unwrap();
    assert_eq!((first.report.proposed, first.report.active, first.report.transitions, first.failed), (2, 0, 0, 0));
    assert!(owner_edges(&p.edges().await).iter().all(|e| e.3 != "active"));
    assert_eq!(p.records().await.iter().filter(|r| r.1 == "transition").count(), 0, "dry run never chains");

    // Reads: dry run (the default) leaves Ask ranking and payloads untouched.
    let tenant = Tenant {
        store: Some(Arc::new(PostgresStore::new(p.app.clone()))),
        pool: Some(p.app.clone()),
        decision: Some(TenancyDecision::Personal),
        class: coralswarm_backend::route_class::RouteClass::Other,
    };
    let user = UserContext { user_id: p.user, account_id: p.account, role: "owner".into(), device_id: "fact-supersession".into() };
    let state = Arc::new(coralswarm_backend::test_app_state_owned_with_db_and_store(p.app.clone(), None));
    coralswarm_backend::test_modern_cached_task_admission(&state, &tenant, &user).await;
    let scope: LibraryScope = serde_json::from_value(json!({"graph_ids":[p.graph]})).unwrap();
    let terms = vec!["apollo".to_string(), "owns".to_string()];
    let ask = |m: &'static str| {
        let (tenant, user, scope, terms) = (tenant.clone(), user.clone(), scope.clone(), terms.clone());
        async move {
            let _g = mode(m);
            library_read::retrieve_ask_candidates(&tenant, &user, scope, &terms, None, None, 10).await.unwrap()
        }
    };
    let quiet = ask("dry_run").await;
    assert_eq!(quiet["outcomes"].as_array().unwrap().len(), 3);
    assert!(quiet["outcomes"].as_array().unwrap().iter().all(|o| o.get("supersession").is_none()));

    {
        let _live = mode("live");
        let promote = BackfillArgs { mode: Mode::Live, ..dry.clone() };
        let second = fact_supersession::backfill(&p.owner, &promote).await.unwrap();
        assert_eq!((second.report.promoted, second.report.transitions, second.report.asserts), (2, 2, 0));
        let mut states: Vec<_> = owner_edges(&p.edges().await).into_iter().map(|e| e.3.to_owned()).collect();
        states.sort();
        assert_eq!(states, ["active", "active", "blocked"]);
        let third = fact_supersession::backfill(&p.owner, &promote).await.unwrap();
        let r = &third.report;
        assert_eq!((r.asserts, r.withdraws, r.transitions, r.proposed, r.active, r.promoted, r.ambiguous, third.failed), (0, 0, 0, 0, 0, 0, 0, 0), "{r:?}");
    }

    // Live reads: the superseded fact ranks after current ones and says so.
    let live = ask("live").await;
    let outcomes = live["outcomes"].as_array().unwrap();
    let ids: Vec<Uuid> = outcomes.iter().map(|o| serde_json::from_value(o["semantic_atom_id"].clone()).unwrap()).collect();
    assert_eq!(*ids.last().unwrap(), s.v1.atom, "the replaced owner ranks last: {live}");
    let x = outcomes.iter().find(|o| o["semantic_atom_id"] == json!(s.v1.atom)).unwrap();
    assert_eq!(x["supersession"]["status"], "superseded");
    let mut by: Vec<Uuid> = x["supersession"]["superseded_by"].as_array().unwrap().iter()
        .map(|b| serde_json::from_value(b["semantic_atom_id"].clone()).unwrap()).collect();
    by.sort();
    let mut expected = vec![s.v2.atom, s.v3.atom];
    expected.sort();
    assert_eq!(by, expected);
    assert_eq!(x["supersession"]["superseded_by"][0]["rule"], "explicit_end");
    for current in [s.v2.atom, s.v3.atom] {
        let o = outcomes.iter().find(|o| o["semantic_atom_id"] == json!(current)).unwrap();
        assert!(o.get("supersession").is_none(), "Z is not superseded: the W pair is blocked");
    }
    // A superseder whose source is restricted stops counting.
    sqlx::query("UPDATE source_items SET access_state='restricted' WHERE source_item_id=ANY($1)")
        .bind(vec![s.v2.item, s.v3.item])
        .execute(&p.owner)
        .await
        .unwrap();
    let restricted = ask("live").await;
    let only: Vec<&Value> = restricted["outcomes"].as_array().unwrap().iter().collect();
    assert_eq!(only.len(), 1);
    assert!(only[0].get("supersession").is_none(), "no hidden superseder is disclosed");
    let _ = (s.apollo, s.x, s.z, s.w);
    drop(state);
    p.done().await;
}

/// Live reads name the superseding fact (statement, source, dates) on both
/// read surfaces (Ask candidates and Library search) for a reader who may open
/// it. A superseder the reader cannot read is never disclosed: not its text,
/// not its id, not a marker. Dry run (production today) shows nothing.
#[tokio::test]
async fn live_reads_name_a_readable_superseder_and_never_disclose_an_unreadable_one() {
    let _lock = ENV.lock().await;
    let Some(opts) = base() else { return };
    let p = Plane::consumer(&opts).await;
    superseder_disclosure(&p).await;
    p.done().await;
}

async fn superseder_disclosure(p: &Plane) {
    let s = story(p).await;
    {
        let _live = mode("live");
        for v in [&s.v1, &s.v2, &s.v3] {
            p.process(v.version, Mode::Live, Role::App).await;
        }
    }
    let tenant = Tenant {
        store: Some(Arc::new(PostgresStore::new(p.app.clone()))),
        pool: Some(p.app.clone()),
        decision: Some(TenancyDecision::Personal),
        class: coralswarm_backend::route_class::RouteClass::Other,
    };
    let user = UserContext { user_id: p.user, account_id: p.account, role: "owner".into(), device_id: "fact-supersession".into() };
    let state = Arc::new(coralswarm_backend::test_app_state_owned_with_db_and_store(p.app.clone(), None));
    coralswarm_backend::test_modern_cached_task_admission(&state, &tenant, &user).await;
    let scope: LibraryScope = serde_json::from_value(json!({"graph_ids":[p.graph]})).unwrap();
    let terms = vec!["apollo".to_string(), "owns".to_string()];
    // Both read surfaces that attach `supersession`, under one mode.
    let reads = |m: &'static str| {
        let (tenant, user, scope, terms) = (tenant.clone(), user.clone(), scope.clone(), terms.clone());
        async move {
            let _g = mode(m);
            let ask = library_read::retrieve_ask_candidates(&tenant, &user, scope.clone(), &terms, None, None, 10).await.unwrap();
            let search = library_read::retrieve_scoped(&tenant, &user, scope, "Apollo", 20).await.unwrap();
            [ask["outcomes"].clone(), search["outcomes"]["items"].clone()]
        }
    };
    let x_of = |outcomes: &Value| outcomes.as_array().unwrap().iter().find(|o| o["semantic_atom_id"] == json!(s.v1.atom)).cloned();

    // Dry run: no surface carries supersession, so nothing names a superseder.
    for outcomes in reads("dry_run").await {
        assert_eq!(outcomes.as_array().unwrap().len(), 3, "{outcomes}");
        assert!(outcomes.as_array().unwrap().iter().all(|o| o.get("supersession").is_none()), "{outcomes}");
    }

    // Live, every superseder readable: each entry names its fact.
    for outcomes in reads("live").await {
        let x = x_of(&outcomes).expect("X is readable");
        let by = x["supersession"]["superseded_by"].as_array().unwrap();
        assert_eq!(by.len(), 2, "{x}");
        for (src, text, occurred) in [(&s.v2, "Zoe Park owns Apollo from 2026-04-01.", "2026-04-02"), (&s.v3, "Wen Li owns Apollo from 2026-05-01.", "2026-05-02")] {
            let entry = by.iter().find(|b| b["semantic_atom_id"] == json!(src.atom)).unwrap_or_else(|| panic!("{x}"));
            assert_eq!(entry["statement"], text, "{entry}");
            assert_eq!(entry["statement_truncated"], false);
            assert_eq!(entry["source_item_id"], json!(src.item));
            let occurred_at: DateTime<Utc> = serde_json::from_value(entry["occurred_at"].clone()).unwrap();
            assert_eq!(occurred_at, at(occurred), "{entry}");
            assert_eq!(entry["rule"], "explicit_end");
        }
    }

    // Live, Zoe's source restricted: her fact is unreadable, so X names only
    // Wen, and nothing of Zoe's fact (text, id) appears anywhere.
    sqlx::query("UPDATE source_items SET access_state='restricted' WHERE source_item_id=$1")
        .bind(s.v2.item).execute(&p.owner).await.unwrap();
    for outcomes in reads("live").await {
        let raw = outcomes.to_string();
        assert!(!raw.contains("Zoe Park") && !raw.contains(&s.v2.atom.to_string()) && !raw.contains(&s.v2.item.to_string()), "unreadable superseder disclosed: {raw}");
        let x = x_of(&outcomes).expect("X is readable");
        let by = x["supersession"]["superseded_by"].as_array().unwrap();
        assert_eq!(by.len(), 1, "{x}");
        assert_eq!((by[0]["semantic_atom_id"].clone(), by[0]["statement"].clone()), (json!(s.v3.atom), json!("Wen Li owns Apollo from 2026-05-01.")));
    }

    // Live, every superseder restricted: X reads as current, with no marker.
    sqlx::query("UPDATE source_items SET access_state='restricted' WHERE source_item_id=$1")
        .bind(s.v3.item).execute(&p.owner).await.unwrap();
    for outcomes in reads("live").await {
        let raw = outcomes.to_string();
        assert!(!raw.contains("Zoe Park") && !raw.contains("Wen Li"), "unreadable superseder disclosed: {raw}");
        let x = x_of(&outcomes).expect("X is readable");
        assert!(x.get("supersession").is_none(), "{x}");
    }
    let _ = (s.apollo, s.x, s.z, s.w);
    drop(state);
}

// ── Reef merges ─────────────────────────────────────────────────────────────
//
// A merge moves the duplicate's semantic links and relationship edges onto
// the keeper, then deletes the duplicate, which cascades its temporal records
// and decisions. The merge re-derives them on the keeper (and on every reef
// whose relation named the duplicate), and its undo restores both sides.

type Supported = BTreeSet<(Uuid, String, String, Option<String>, Uuid)>;
type Decided = BTreeSet<(Uuid, String, Uuid, Uuid, String, String)>;

impl Plane {
    fn store(&self, role: Role) -> PostgresStore {
        PostgresStore::new(if role == Role::Owner { self.owner.clone() } else { self.app.clone() })
    }

    async fn merge(&self, role: Role, keep: Uuid, dup: Uuid) -> Uuid {
        let mut tx = self.begin(role).await;
        self.store(role).merge_nodes_scoped(&mut tx, keep, dup).await.unwrap();
        tx.commit().await.unwrap();
        sqlx::query_scalar("SELECT merge_id FROM node_merges WHERE keep_id=$1 AND dup_id=$2 AND undone_at IS NULL")
            .bind(keep)
            .bind(dup)
            .fetch_one(&self.owner)
            .await
            .unwrap()
    }

    async fn unmerge(&self, role: Role, merge_id: Uuid) {
        let mut tx = self.begin(role).await;
        self.store(role).unmerge_nodes_scoped(&mut tx, merge_id).await.unwrap();
        tx.commit().await.unwrap();
    }

    /// Supported extraction records: (subject, key, operation, value, atom).
    async fn supported(&self) -> Supported {
        let rows: Vec<(Uuid, String, String, Option<String>, Uuid)> = sqlx::query_as(
            "SELECT subject_node_id,state_key,operation,value,semantic_atom_id FROM temporal_evidence_records \
              WHERE graph_id=$1 AND recording_method='temporal_extraction_v1' AND lifecycle='supported'",
        )
        .bind(self.graph)
        .fetch_all(&self.owner)
        .await
        .unwrap();
        rows.into_iter().collect()
    }

    /// Decisions: (subject, key, superseder atom, superseded atom, rule, status).
    async fn decided(&self) -> Decided {
        let rows: Vec<(Uuid, String, Uuid, Uuid, String, String)> = sqlx::query_as(
            "SELECT subject_node_id,state_key,superseder_atom_id,superseded_atom_id,rule,status FROM fact_supersession_edges WHERE graph_id=$1",
        )
        .bind(self.graph)
        .fetch_all(&self.owner)
        .await
        .unwrap();
        rows.into_iter().collect()
    }

    /// Every supported transition's predecessor is a supported record on the
    /// same subject and key (what `temporal::read_tx` needs to end a state).
    async fn dangling_chains(&self) -> i64 {
        self.count(
            "SELECT count(*) FROM temporal_evidence_records t WHERE t.graph_id=$1 AND t.lifecycle='supported' \
               AND t.predecessor_event_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM temporal_evidence_records p \
               WHERE p.event_id=t.predecessor_event_id AND p.lifecycle='supported' AND p.subject_node_id=t.subject_node_id)",
        )
        .await
    }
}

/// Apollo was minted twice. The copy (`Apollo Project`) holds "Xavier owns
/// Apollo until March" and is the value of "Ana works on Apollo from
/// January"; the keeper holds "Zoe owns Apollo from April".
struct MergeStory {
    apollo: Uuid,
    copy: Uuid,
    ana: Uuid,
    x_end: Src,
    z_start: Src,
    ana_work: Src,
}

async fn merge_story(p: &Plane, role: Role, m: Mode) -> MergeStory {
    let apollo = p.node("Apollo").await;
    let copy = p.node("Apollo Project").await;
    let x = p.node("Xavier Ortiz").await;
    let z = p.node("Zoe Park").await;
    let ana = p.node("Ana Ruiz").await;
    let x_end = p.source("m-own-1", "Xavier Ortiz owns Apollo until 2026-03-01.", "2026-02-01", (None, Some("2026-03-01"), Some("until 2026-03-01")), &[x, copy]).await;
    p.relation(x, "responsible_for", copy, x_end.atom, None, Some("2026-03-01"), Some("until 2026-03-01")).await;
    let z_start = p.source("m-own-2", "Zoe Park owns Apollo from 2026-04-01.", "2026-04-02", (Some("2026-04-01"), None, Some("from 2026-04-01")), &[z, apollo]).await;
    p.relation(z, "responsible_for", apollo, z_start.atom, Some("2026-04-01"), None, Some("from 2026-04-01")).await;
    let ana_work = p.source("m-work-1", "Ana Ruiz works on Apollo from 2026-01-01.", "2026-01-02", (Some("2026-01-01"), None, Some("from 2026-01-01")), &[ana, copy]).await;
    p.relation(ana, "works_on", copy, ana_work.atom, Some("2026-01-01"), None, Some("from 2026-01-01")).await;
    for v in [&x_end, &z_start, &ana_work] {
        p.process(v.version, m, role).await;
    }
    MergeStory { apollo, copy, ana, x_end, z_start, ana_work }
}

fn on(set: &Supported, subject: Uuid, key: &str) -> Vec<(String, Option<String>, Uuid)> {
    set.iter().filter(|r| r.0 == subject && r.1 == key).map(|r| (r.2.clone(), r.3.clone(), r.4)).collect()
}

async fn merge_rederives_and_undo_restores(p: &Plane, role: Role) {
    let _g = mode("live");
    let s = merge_story(p, role, Mode::Live).await;
    let before = (p.supported().await, p.decided().await);
    assert_eq!(on(&before.0, s.copy, "rel.owner").len(), 2, "the copy holds Xavier's assert and withdraw");
    assert!(before.1.iter().all(|d| d.1 != "rel.owner"), "different subjects: nothing decided yet");

    let merge_id = p.merge(role, s.apollo, s.copy).await;
    let merged = (p.supported().await, p.decided().await);

    // The keeper now holds the copy's history, and the replacement the two
    // facts together imply: Zoe replaced Xavier (explicit end), chained.
    let mut owner = on(&merged.0, s.apollo, "rel.owner");
    owner.sort();
    let mut expected = vec![
        ("assert".to_string(), Some("Xavier Ortiz".to_string()), s.x_end.atom),
        ("assert".to_string(), Some("Zoe Park".to_string()), s.z_start.atom),
        ("transition".to_string(), Some("Zoe Park".to_string()), s.z_start.atom),
        ("withdraw".to_string(), None, s.x_end.atom),
    ];
    expected.sort();
    assert_eq!(owner, expected, "keeper rel.owner after merge: {merged:?}");
    assert!(merged.1.contains(&(s.apollo, "rel.owner".into(), s.z_start.atom, s.x_end.atom, "explicit_end".into(), "active".into())), "{:?}", merged.1);
    for atom in [s.x_end.atom, s.ana_work.atom, s.z_start.atom] {
        assert!(merged.0.iter().any(|r| r.0 == s.apollo && r.1 == "fact.fact" && r.2 == "assert" && r.4 == atom), "atom fact {atom} on the keeper");
    }
    // Ana's relation named the copy; it now names the keeper, once.
    assert_eq!(on(&merged.0, s.ana, "rel.works_on"), vec![("assert".to_string(), Some("Apollo".to_string()), s.ana_work.atom)]);
    assert!(merged.0.iter().all(|r| r.0 != s.copy), "nothing survives on the deleted copy");
    assert_eq!(p.dangling_chains().await, 0);

    // Undo restores both reefs exactly.
    p.unmerge(role, merge_id).await;
    let undone = (p.supported().await, p.decided().await);
    assert_eq!(undone.0, before.0, "records after undo");
    assert_eq!(undone.1, before.1, "decisions after undo");
    assert_eq!(p.dangling_chains().await, 0);

    // Merging again lands where the first merge did (idempotent re-derivation).
    p.merge(role, s.apollo, s.copy).await;
    assert_eq!((p.supported().await, p.decided().await), merged, "a second merge after undo");
}

/// A stated change on a duplicate reef follows the merge: its facts are
/// re-derived on the keeper (where it now also replaces the keeper's earlier
/// owner), nothing is retracted for it, undo restores both reefs, and a
/// second merge lands where the first did.
async fn merge_carries_stated_changes(p: &Plane, role: Role) {
    let _g = mode("live");
    let atlas = p.node("Atlas").await;
    let copy = p.node("Atlas Project").await;
    let bo = p.node("Bo Chen").await;
    let ana = p.node("Ana Ruiz").await;
    let prior = p.source("mc-own-0", "Bo Chen owns Atlas.", "2026-02-01", (None, None, None), &[bo, atlas]).await;
    p.relation(bo, "responsible_for", atlas, prior.atom, None, None, None).await;
    let text = "Atlas Project was previously owned by Bo Chen; since 2026-03-01 Ana Ruiz owns it.";
    let change = p.source("mc-own-1", text, "2026-04-01", (None, None, None), &[copy, ana, bo]).await;
    p.relation(ana, "responsible_for", copy, change.atom, Some("2026-03-01"), None, Some("since 2026-03-01")).await;
    p.stated(&change, json!([stated_change("Atlas Project", copy, "rel.owner", Some(("Bo Chen", Some(bo))), ("Ana Ruiz", Some(ana)), Some("2026-03-01"), text)])).await;
    for v in [&prior, &change] {
        p.process(v.version, Mode::Live, role).await;
    }
    let before = (p.supported().await, p.decided().await);
    assert!(before.1.contains(&(copy, "rel.owner".into(), change.atom, change.atom, "stated_change".into(), "active".into())), "{:?}", before.1);
    assert!(before.1.iter().all(|d| d.0 != atlas), "different subjects: nothing decided on Atlas yet");

    let merge_id = p.merge(role, atlas, copy).await;
    let merged = (p.supported().await, p.decided().await);
    let mut owner = on(&merged.0, atlas, "rel.owner");
    owner.sort();
    let mut expected = vec![
        ("assert".to_string(), Some("Ana Ruiz".to_string()), change.atom),
        ("assert".to_string(), Some("Bo Chen".to_string()), change.atom),
        ("assert".to_string(), Some("Bo Chen".to_string()), prior.atom),
        ("transition".to_string(), Some("Ana Ruiz".to_string()), change.atom),
        ("withdraw".to_string(), None, change.atom),
    ];
    expected.sort();
    assert_eq!(owner, expected, "keeper rel.owner after merge: {merged:?}");
    // The change chains to the keeper's replaced assert only: its own old
    // value comes from the same atom and never ends a state.
    let transitions: i64 = sqlx::query_scalar("SELECT count(*) FROM temporal_evidence_records WHERE graph_id=$1 AND subject_node_id=$2 \
          AND state_key='rel.owner' AND operation='transition' AND lifecycle='supported'")
        .bind(p.graph).bind(atlas).fetch_one(&p.owner).await.unwrap();
    assert_eq!(transitions, 1);
    for decision in [
        (atlas, "rel.owner".to_string(), change.atom, change.atom, "stated_change".to_string(), "active".to_string()),
        (atlas, "rel.owner".to_string(), change.atom, prior.atom, "stated_change".to_string(), "active".to_string()),
    ] {
        assert!(merged.1.contains(&decision), "{decision:?} in {:?}", merged.1);
    }
    assert!(merged.0.iter().all(|r| r.0 != copy) && merged.1.iter().all(|d| d.0 != copy), "nothing survives on the deleted copy");
    assert_eq!(p.dangling_chains().await, 0);

    p.unmerge(role, merge_id).await;
    assert_eq!((p.supported().await, p.decided().await), before, "undo restores both reefs");
    p.merge(role, atlas, copy).await;
    assert_eq!((p.supported().await, p.decided().await), merged, "a second merge after undo");
}

#[tokio::test]
async fn tenant_merge_carries_stated_changes_as_app_role() {
    let _lock = ENV.lock().await;
    let Some(opts) = base() else { return };
    let p = Plane::tenant(&opts).await;
    merge_carries_stated_changes(&p, Role::App).await;
    p.done().await;
}

#[tokio::test]
async fn tenant_merge_carries_stated_changes_as_table_owner() {
    let _lock = ENV.lock().await;
    let Some(opts) = base() else { return };
    let p = Plane::tenant(&opts).await;
    merge_carries_stated_changes(&p, Role::Owner).await;
    p.done().await;
}

#[tokio::test]
async fn tenant_merge_rederives_temporal_evidence_and_undo_restores_as_app_role() {
    let _lock = ENV.lock().await;
    let Some(opts) = base() else { return };
    let p = Plane::tenant(&opts).await;
    merge_rederives_and_undo_restores(&p, Role::App).await;
    p.done().await;
}

#[tokio::test]
async fn tenant_merge_rederives_temporal_evidence_and_undo_restores_as_table_owner() {
    let _lock = ENV.lock().await;
    let Some(opts) = base() else { return };
    let p = Plane::tenant(&opts).await;
    merge_rederives_and_undo_restores(&p, Role::Owner).await;
    p.done().await;
}

#[tokio::test]
async fn consumer_merge_rederives_temporal_evidence_and_undo_restores_as_app_role() {
    let _lock = ENV.lock().await;
    let Some(opts) = base() else { return };
    let p = Plane::consumer(&opts).await;
    merge_rederives_and_undo_restores(&p, Role::App).await;
    p.done().await;
}

/// Off: a merge records and decides nothing (the copy's history goes with
/// it). Dry run: evidence and a `proposed` decision, never a transition.
#[tokio::test]
async fn merge_rederivation_respects_the_mode() {
    let _lock = ENV.lock().await;
    let Some(opts) = base() else { return };
    let p = Plane::tenant(&opts).await;
    let s = merge_story(&p, Role::App, Mode::Live).await;
    {
        let _g = mode("off");
        p.merge(Role::App, s.apollo, s.copy).await;
    }
    let supported = p.supported().await;
    assert!(supported.iter().all(|r| r.0 != s.apollo || r.4 != s.x_end.atom), "off: nothing re-derived on the keeper");
    assert_eq!(on(&supported, s.ana, "rel.works_on"), vec![("assert".to_string(), Some("Apollo Project".to_string()), s.ana_work.atom)], "off: nothing retracted");
    p.done().await;

    let p = Plane::tenant(&opts).await;
    let s = merge_story(&p, Role::App, Mode::DryRun).await;
    {
        let _g = mode("dry_run");
        p.merge(Role::App, s.apollo, s.copy).await;
    }
    let supported = p.supported().await;
    assert!(supported.iter().any(|r| r.0 == s.apollo && r.1 == "rel.owner" && r.2 == "assert" && r.4 == s.x_end.atom));
    assert!(supported.iter().all(|r| r.2 != "transition"), "dry run never chains");
    let decided = p.decided().await;
    assert!(decided.contains(&(s.apollo, "rel.owner".into(), s.z_start.atom, s.x_end.atom, "explicit_end".into(), "proposed".into())), "{decided:?}");
    p.done().await;
}

/// A judged outcome on the copy is carried to the keeper's re-derived
/// decision instead of sending the same pair to the judge again.
#[tokio::test]
async fn merge_carries_judged_outcomes_to_the_keeper() {
    let _lock = ENV.lock().await;
    let Some(opts) = base() else { return };
    let p = Plane::tenant(&opts).await;
    let _g = mode("live");
    let ana = p.node("Ana Ruiz").await;
    let copy = p.node("Ana R.").await;
    let apollo = p.node("Apollo").await;
    let hermes = p.node("Hermes").await;
    let a = p.source("c-work-1", "Ana Ruiz works at Apollo.", "2026-02-01", (None, None, None), &[copy, apollo]).await;
    p.relation(copy, "works_at", apollo, a.atom, None, None, None).await;
    let b = p.source("c-work-2", "Ana Ruiz works at Hermes from 2026-04-01.", "2026-04-02", (None, None, None), &[copy, hermes]).await;
    p.relation(copy, "works_at", hermes, b.atom, Some("2026-04-01"), None, Some("from 2026-04-01")).await;
    p.process(a.version, Mode::Live, Role::App).await;
    p.process(b.version, Mode::Live, Role::App).await;
    sqlx::query("UPDATE fact_supersession_edges SET status='rejected',judge_attempts=1 WHERE graph_id=$1 AND status='ambiguous'")
        .bind(p.graph)
        .execute(&p.owner)
        .await
        .unwrap();
    p.merge(Role::App, ana, copy).await;
    let decided = p.decided().await;
    assert_eq!(decided.into_iter().collect::<Vec<_>>(), vec![(ana, "rel.employer".into(), b.atom, a.atom, "judge".into(), "rejected".into())]);
    p.done().await;
}

/// The repair for merges made before the hook: a merge with the mode off
/// loses the copy's history (as production's did). The repair previews
/// without writing, then restores the keeper and retires the relation that
/// named the copy; a second pass, and a pass over a hooked merge, write
/// nothing.
#[tokio::test]
async fn repair_merges_restores_keepers_that_lost_evidence_and_is_idempotent() {
    let _lock = ENV.lock().await;
    let Some(opts) = base() else { return };
    let p = Plane::tenant(&opts).await;
    let s = merge_story(&p, Role::App, Mode::Live).await;
    {
        let _g = mode("off");
        p.merge(Role::App, s.apollo, s.copy).await;
    }
    let lost = (p.supported().await, p.decided().await);
    let args = BackfillArgs { execute: false, mode: Mode::Live, graph_id: Some(p.graph), after: None, limit: 100 };
    let preview = fact_supersession::repair_merges(&p.owner, &args).await.unwrap();
    assert_eq!((preview.candidates, preview.merges, preview.failed), (1, 1, 0));
    let r = &preview.report;
    assert_eq!((r.asserts, r.withdraws, r.active, r.transitions, r.retracted), (4, 2, 1, 1, 1), "{r:?}");
    assert_eq!((p.supported().await, p.decided().await), lost, "a dry-run repair writes nothing");

    let _g = mode("live");
    let repaired = fact_supersession::repair_merges(&p.owner, &BackfillArgs { execute: true, ..args.clone() }).await.unwrap();
    assert_eq!((repaired.report.asserts, repaired.report.active, repaired.report.retracted, repaired.failed), (4, 1, 1, 0));
    let supported = p.supported().await;
    let mut owner = on(&supported, s.apollo, "rel.owner");
    owner.sort();
    assert_eq!(owner.iter().map(|r| r.0.as_str()).collect::<Vec<_>>(), ["assert", "assert", "transition", "withdraw"]);
    assert_eq!(on(&supported, s.ana, "rel.works_on"), vec![("assert".to_string(), Some("Apollo".to_string()), s.ana_work.atom)]);
    assert!(p.decided().await.contains(&(s.apollo, "rel.owner".into(), s.z_start.atom, s.x_end.atom, "explicit_end".into(), "active".into())));
    assert_eq!(p.dangling_chains().await, 0);

    let again = fact_supersession::repair_merges(&p.owner, &BackfillArgs { execute: true, ..args.clone() }).await.unwrap();
    let r = &again.report;
    assert_eq!((r.asserts, r.withdraws, r.transitions, r.retracted, r.decisions_removed, r.proposed, r.active, r.ambiguous), (0, 0, 0, 0, 0, 0, 0, 0), "{r:?}");
    p.done().await;

    // A merge the hook already reconciled leaves the repair nothing to do.
    let p = Plane::tenant(&opts).await;
    let s = merge_story(&p, Role::Owner, Mode::Live).await;
    p.merge(Role::Owner, s.apollo, s.copy).await;
    let hooked = fact_supersession::repair_merges(&p.owner, &BackfillArgs { execute: true, graph_id: Some(p.graph), ..args.clone() }).await.unwrap();
    let r = &hooked.report;
    assert_eq!((hooked.merges, r.asserts, r.withdraws, r.transitions, r.retracted, r.decisions_removed, r.active), (1, 0, 0, 0, 0, 0, 0), "{r:?}");
    p.done().await;
}

/// `dedupe_reefs` merges a whole group (584 copies in production) in one
/// transaction. Two shapes: each copy states a dated owner of Apollo (a
/// single-valued key, the worst case: every merge decides against up to
/// PRIOR_LIMIT prior owners on the keeper and chains each replacement), or
/// a dated project the reef works on (many-valued, the common case). The
/// added cost per merge stays flat as the keeper grows: the hook reads the
/// copy's facts and a bounded prior window, never the keeper's history. The
/// first `warm` copies fill that window (a relation's priors are read up to
/// 2 x PRIOR_LIMIT rows), then the halves of the measured merges compare.
/// `FACT_SUPERSESSION_MERGE_GROUP` sets the measured size (default 40).
#[tokio::test]
async fn large_group_merge_cost_stays_bounded() {
    let _lock = ENV.lock().await;
    let Some(opts) = base() else { return };
    let n: usize = std::env::var("FACT_SUPERSESSION_MERGE_GROUP").ok().and_then(|v| v.parse().ok()).unwrap_or(40);
    let warm = 2 * fact_supersession::PRIOR_LIMIT as usize + 8;
    for predicate in ["responsible_for", "works_on"] {
        let key = if predicate == "works_on" { "rel.works_on" } else { "rel.owner" };
        let mut totals = Vec::new();
        for m in ["off", "live"] {
            let p = Plane::tenant(&opts).await;
            let keep = p.node("Apollo").await;
            let mut copies = Vec::new();
            for i in 0..warm + n {
                let copy = p.node(&format!("Apollo copy {i}")).await;
                let other = p.node(&format!("Other {i}")).await;
                let day = (at("2024-01-01") + chrono::Duration::days(i as i64)).format("%Y-%m-%d").to_string();
                let text = format!("Apollo and Other {i}, from {day}.");
                let src = p.source(&format!("g-{i}"), &text, &day, (Some(&day), None, Some("from")), &[other, copy]).await;
                if predicate == "works_on" {
                    p.relation(copy, predicate, other, src.atom, Some(&day), None, Some("from")).await;
                } else {
                    p.relation(other, predicate, copy, src.atom, Some(&day), None, Some("from")).await;
                }
                p.process(src.version, Mode::Live, Role::App).await;
                copies.push(copy);
            }
            let _g = mode(m);
            let store = p.store(Role::App);
            let mut tx = p.begin(Role::App).await;
            let mut each = Vec::new();
            for (i, copy) in copies.iter().enumerate() {
                let started = std::time::Instant::now();
                store.merge_nodes_scoped(&mut tx, keep, *copy).await.unwrap();
                if i >= warm {
                    each.push(started.elapsed().as_secs_f64() * 1000.0);
                }
            }
            tx.commit().await.unwrap();
            let half = |h: &[f64]| h.iter().sum::<f64>() / h.len() as f64;
            let (first, second) = (half(&each[..n / 2]), half(&each[n / 2..]));
            let total: f64 = each.iter().sum();
            let supported = p.supported().await;
            let asserts = on(&supported, keep, key).iter().filter(|r| r.0 == "assert").count();
            let decided = p.decided().await.iter().filter(|d| d.0 == keep).count();
            eprintln!("merge group {predicate} warm={warm} n={n} mode={m}: {:.2}s measured, {:.1} ms/merge (first half {first:.1}, second half {second:.1}); keeper asserts={asserts} decisions={decided}",
                total / 1000.0, total / n as f64);
            if m == "live" {
                let copies = warm + n;
                assert_eq!(asserts, copies, "every copy's relation is on the keeper");
                assert!(decided <= copies * (2 * fact_supersession::PRIOR_LIMIT as usize + 1), "decisions bounded by the prior window: {decided}");
                if predicate == "responsible_for" {
                    assert!(decided >= copies - 1, "each later start replaces an earlier one: {decided}");
                }
                assert_eq!(p.dangling_chains().await, 0);
                // Flat, not growing with the keeper: with the window full,
                // the second half costs about what the first did.
                assert!(second < first * 2.0 + 25.0, "per-merge cost grows with the keeper: {first:.1} -> {second:.1} ms");
            } else {
                assert_eq!(asserts, 0);
            }
            totals.push(total);
            p.done().await;
        }
        eprintln!("merge group {predicate}: added cost per merge {:.1} ms", (totals[1] - totals[0]).max(0.0) / n as f64);
    }
}

// ── Stated changes (extraction v3) ──────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
fn stated_change(subject: &str, subject_node: Uuid, key: &str, old: Option<(&str, Option<Uuid>)>, new: (&str, Option<Uuid>), from: Option<&str>, quote: &str) -> Value {
    json!({"subject": subject, "subject_node_id": subject_node, "attribute": key.trim_start_matches("rel.").trim_start_matches("attr."),
        "state_key": key, "functional": true,
        "old_value": old.map(|o| o.0), "old_node_id": old.and_then(|o| o.1),
        "new_value": new.0, "new_node_id": new.1,
        "effective_from": from.map(at), "effective_quote": from.map(|f| format!("since {f}")), "quote": quote})
}

/// One sentence states each change. Bo owns Atlas (an earlier, undated
/// source); then "Atlas was previously owned by Bo Chen; since 2026-03-01
/// Ana Ruiz owns it"; and an undated free-text change on deploy.yml.
/// Returns (earlier owner, change, later restatement) atoms.
async fn stated_change_story(p: &Plane, role: Role) -> (Uuid, Uuid, Uuid) {
    let atlas = p.node("Atlas").await;
    let bo = p.node("Bo Chen").await;
    let ana = p.node("Ana Ruiz").await;
    let deploy = p.node("deploy.yml").await;
    let prior = p.source("own-0", "Bo Chen owns Atlas.", "2026-02-01", (None, None, None), &[bo, atlas]).await;
    p.relation(bo, "responsible_for", atlas, prior.atom, None, None, None).await;
    let text = "Atlas was previously owned by Bo Chen; since 2026-03-01 Ana Ruiz owns it.";
    let change = p.source("own-1", text, "2026-04-01", (None, None, None), &[atlas, ana, bo]).await;
    // The new owner's edge, as v3 states it from the change.
    p.relation(ana, "responsible_for", atlas, change.atom, Some("2026-03-01"), None, Some("since 2026-03-01")).await;
    p.stated(&change, json!([stated_change("Atlas", atlas, "rel.owner", Some(("Bo Chen", Some(bo))), ("Ana Ruiz", Some(ana)), Some("2026-03-01"), text)])).await;
    let ci = "deploy.yml previously fired on CI and is now manual dispatch.";
    let trigger = p.source("ci-1", ci, "2026-04-02", (None, None, None), &[deploy]).await;
    p.stated(&trigger, json!([stated_change("deploy.yml", deploy, "attr.deploy_trigger", Some(("CI", None)), ("manual dispatch", None), None, ci)])).await;

    // Dry run: each sentence yields its proposals; nothing is active.
    for v in [&prior, &change, &trigger] {
        p.process(v.version, Mode::DryRun, role).await;
    }
    let mut edges: Vec<(String, Uuid, Uuid, String, String)> = p.edges().await.into_iter().map(|e| (e.0, e.1, e.2, e.3, e.4)).collect();
    edges.sort();
    let mut expected = vec![
        ("attr.deploy_trigger".to_string(), trigger.atom, trigger.atom, "stated_change".to_string(), "proposed".to_string()),
        ("rel.owner".into(), change.atom, prior.atom, "stated_change".into(), "proposed".into()),
        ("rel.owner".into(), change.atom, change.atom, "stated_change".into(), "proposed".into()),
    ];
    expected.sort();
    assert_eq!(edges, expected, "a one-sentence change supersedes its own old value and the earlier fact it names");
    let records = p.records().await;
    let owner: Vec<_> = records.iter().filter(|r| r.0 == "rel.owner" && r.5 == change.atom).map(|r| (r.1.as_str(), r.2.as_deref(), r.3, r.4)).collect();
    assert!(owner.contains(&("assert", Some("Ana Ruiz"), Some(at("2026-03-01")), None)), "{owner:?}");
    assert!(owner.contains(&("assert", Some("Bo Chen"), None, None)) && owner.contains(&("withdraw", None, Some(at("2026-03-01")), None)),
        "the dated old value is asserted and ended at the change: {owner:?}");
    let ci_records: Vec<_> = records.iter().filter(|r| r.0 == "attr.deploy_trigger").map(|r| (r.1.as_str(), r.2.as_deref())).collect();
    assert_eq!(ci_records, vec![("assert", Some("manual dispatch"))], "an undated old value records no evidence, only the decision");

    // Live: proposals promote and the dated change chains its transitions.
    for v in [&prior, &change, &trigger] {
        p.process(v.version, Mode::Live, role).await;
    }
    let states: Vec<String> = p.edges().await.into_iter().map(|e| e.4).collect();
    assert!(states.iter().all(|s| s == "active"), "{states:?}");
    let transitions: Vec<_> = p.records().await.into_iter().filter(|r| r.1 == "transition").map(|r| (r.0, r.5, r.2)).collect();
    assert_eq!(transitions, vec![("rel.owner".to_string(), change.atom, Some("Ana Ruiz".to_string()))],
        "the earlier source's assert is ended; the sentence's own old value (one atom) chains nothing");

    // Idempotent per version.
    let before = (p.records().await.len(), p.edges().await.len());
    for v in [&prior, &change, &trigger] {
        let again = p.process(v.version, Mode::Live, role).await;
        assert_eq!((again.asserts, again.withdraws, again.transitions, again.active, again.proposed), (0, 0, 0, 0, 0));
    }
    assert_eq!((p.records().await.len(), p.edges().await.len()), before);

    // A later source stating the old value again is not superseded by the
    // earlier change.
    let back = p.source("own-2", "Bo Chen owns Atlas.", "2026-09-01", (None, None, None), &[bo, atlas]).await;
    p.relation(bo, "responsible_for", atlas, back.atom, None, None, None).await;
    p.process(back.version, Mode::Live, role).await;
    assert!(p.edges().await.iter().all(|e| e.2 != back.atom || e.4 != "active"), "{:?}", p.edges().await);
    (prior.atom, change.atom, back.atom)
}

#[tokio::test]
async fn tenant_stated_change_supersedes_in_one_sentence_as_app_role() {
    let _lock = ENV.lock().await;
    let Some(opts) = base() else { return };
    let p = Plane::tenant(&opts).await;
    let _ = stated_change_story(&p, Role::App).await;
    p.done().await;
}

#[tokio::test]
async fn tenant_stated_change_supersedes_in_one_sentence_as_table_owner() {
    let _lock = ENV.lock().await;
    let Some(opts) = base() else { return };
    let p = Plane::tenant(&opts).await;
    let owns: bool = sqlx::query_scalar(
        "SELECT tableowner=current_user FROM pg_tables WHERE schemaname='public' AND tablename='fact_supersession_edges'",
    )
    .fetch_one(&p.owner)
    .await
    .unwrap();
    assert!(owns, "the production tenant DSN shape: the connecting role owns the table");
    let _ = stated_change_story(&p, Role::Owner).await;
    p.done().await;
}

/// Consumer plane, and the read side: in live mode the earlier owner's atom
/// is superseded, while the sentence that states the change is not (its own
/// pair never marks it).
#[tokio::test]
async fn consumer_stated_change_supersedes_in_one_sentence_and_reads_keep_the_change_current() {
    let _lock = ENV.lock().await;
    let Some(opts) = base() else { return };
    let p = Plane::consumer(&opts).await;
    let (prior, change, back) = stated_change_story(&p, Role::App).await;
    let tenant = Tenant {
        store: Some(Arc::new(PostgresStore::new(p.app.clone()))),
        pool: Some(p.app.clone()),
        decision: Some(TenancyDecision::Personal),
        class: coralswarm_backend::route_class::RouteClass::Other,
    };
    let user = UserContext { user_id: p.user, account_id: p.account, role: "owner".into(), device_id: "fact-supersession".into() };
    let state = Arc::new(coralswarm_backend::test_app_state_owned_with_db_and_store(p.app.clone(), None));
    coralswarm_backend::test_modern_cached_task_admission(&state, &tenant, &user).await;
    let scope: LibraryScope = serde_json::from_value(json!({"graph_ids":[p.graph]})).unwrap();
    let _live = mode("live");
    let found = library_read::retrieve_ask_candidates(&tenant, &user, scope, &["atlas".to_string(), "owns".to_string()], None, None, 10).await.unwrap();
    let outcomes = found["outcomes"].as_array().unwrap();
    let id = |o: &Value| serde_json::from_value::<Uuid>(o["semantic_atom_id"].clone()).unwrap();
    let superseded: Vec<Uuid> = outcomes.iter().filter(|o| o.get("supersession").is_some()).map(id).collect();
    assert_eq!(superseded, vec![prior], "only the earlier owner is superseded: {found}");
    let stating = outcomes.iter().find(|o| id(o) == change).expect("the change is found");
    assert!(stating.get("supersession").is_none(), "the sentence stating the change stays current");
    assert!(outcomes.iter().any(|o| id(o) == back), "the later restatement is found and not superseded");
    drop(state);
    p.done().await;
}

// ── The judge ───────────────────────────────────────────────────────────────

struct Judge {
    answer: String,
    calls: Mutex<Vec<CompletionRequest>>,
}
#[async_trait::async_trait]
impl LlmProvider for Judge {
    async fn complete(&self, req: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        self.calls.lock().unwrap().push(req);
        Ok(CompletionResponse {
            content: self.answer.clone(),
            input_tokens: 20,
            output_tokens: 8,
            model: "deepseek/deepseek-v4-flash-0731".into(),
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
            stop_reason: Some("tool_use".into()),
            cost_usd: Some(0.00003),
        })
    }
    async fn stream_complete(
        &self,
        _: CompletionRequest,
    ) -> Result<std::pin::Pin<Box<dyn futures::Stream<Item = Result<StreamEvent, LlmError>> + Send>>, LlmError> {
        panic!("forced nonstreaming tool only")
    }
}

/// A single-valued key where only the newer fact is dated is a signal, not
/// a decision: it waits as `ambiguous`. The judge sends nothing while either
/// switch is off, and when both are on it calls Jev once through the managed
/// gate under `managed_enrichment`, binds both atoms as inputs and publishes.
#[tokio::test]
async fn ambiguous_pairs_are_judged_only_under_managed_enrichment() {
    let _lock = ENV.lock().await;
    let Some(opts) = base() else { return };
    let p = Plane::consumer(&opts).await;
    let ana = p.node("Ana Ruiz").await;
    let apollo = p.node("Apollo").await;
    let hermes = p.node("Hermes").await;
    let a = p.source("work-1", "Ana Ruiz works at Apollo.", "2026-02-01", (None, None, None), &[ana, apollo]).await;
    p.relation(ana, "works_at", apollo, a.atom, None, None, None).await;
    let b = p.source("work-2", "Ana Ruiz works at Hermes from 2026-04-01.", "2026-04-02", (None, None, None), &[ana, hermes]).await;
    p.relation(ana, "works_at", hermes, b.atom, Some("2026-04-01"), None, Some("from 2026-04-01")).await;
    let first = p.process(a.version, Mode::Live, Role::App).await;
    let second = p.process(b.version, Mode::Live, Role::App).await;
    assert_eq!((first.ambiguous + second.ambiguous, first.active + second.active), (1, 0), "one question per pair");
    assert_eq!(p.edges().await.iter().map(|e| (e.0.as_str(), e.3.as_str(), e.4.as_str())).collect::<Vec<_>>(),
        vec![("rel.employer", "judge", "ambiguous")]);

    let model = Judge { answer: json!({"q0":{"choice":"supersede","confidence":0.92}}).to_string(), calls: Mutex::new(Vec::new()) };
    let gated = coralswarm_backend::llm::testing::GatedScript(&model);
    let worker = WorkerTenant { tenant: TenantId::Consumer, pool: p.app.clone(), store: Arc::new(PostgresStore::new(p.app.clone())) };
    let receipts = "SELECT count(*) FROM managed_attempt_receipts WHERE graph_id=$1 AND purpose='managed_enrichment'";
    // Switch off: enrichment disabled, or supersession not live. Nothing is sent.
    for (m, enrichment) in [("live", None), ("dry_run", Some("true")), ("off", Some("true"))] {
        let _g = EnvGuard::set(&[
            (fact_supersession::MODE_ENV, Some(m)),
            ("ENRICHMENT_PROCESSING_ENABLED", enrichment),
            ("MANAGED_PROCESSING_DISABLED", None),
        ]);
        let tick = judge::tick(&worker, &p.owner, &gated).await.unwrap();
        assert_eq!(tick.processed, 0, "{m}/{enrichment:?}");
    }
    assert!(model.calls.lock().unwrap().is_empty());
    assert_eq!(p.count(receipts).await, 0);
    assert_eq!(p.edges().await[0].4, "ambiguous");

    // Both on: one metered Jev call.
    let _g = EnvGuard::set(&[
        (fact_supersession::MODE_ENV, Some("live")),
        ("ENRICHMENT_PROCESSING_ENABLED", Some("true")),
        ("MANAGED_PROCESSING_DISABLED", None),
        ("MANAGED_DAILY_CAP_USD_ENRICHMENT", None),
    ]);
    let tick = judge::tick(&worker, &p.owner, &gated).await.unwrap();
    assert_eq!(tick.processed, 1);
    let calls = model.calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 1);
    assert!(calls[0].messages[0].content.contains("fact_supersession") && calls[0].messages[0].content.contains("Hermes"));
    let edge = &p.edges().await[0];
    assert_eq!((edge.1, edge.2, edge.3.as_str(), edge.4.as_str()), (b.atom, a.atom, "judge", "active"));
    let receipt: (String, Option<Uuid>, String) = sqlx::query_as(
        "SELECT r.purpose,r.job_id,r.state FROM managed_attempt_receipts r JOIN fact_supersession_edges f ON f.judge_receipt_id=r.receipt_id WHERE f.graph_id=$1",
    )
    .bind(p.graph)
    .fetch_one(&p.owner)
    .await
    .unwrap();
    assert_eq!(receipt, ("managed_enrichment".into(), None, "billed".into()));
    let inputs: Vec<(String, Uuid, String)> = sqlx::query_as(
        "SELECT input_kind,input_id,outcome FROM managed_enrichment_inputs WHERE graph_id=$1 ORDER BY ordinal",
    )
    .bind(p.graph)
    .fetch_all(&p.owner)
    .await
    .unwrap();
    assert_eq!(inputs, vec![("semantic_atom".into(), b.atom, "written".into()), ("semantic_atom".into(), a.atom, "written".into())]);
    let usage: i64 = sqlx::query_scalar("SELECT count(*) FROM llm_usage WHERE task=$1").bind(judge::TASK).fetch_one(&p.owner).await.unwrap();
    assert_eq!(usage, 1);
    // The judge's replacement chains like a deterministic one.
    let chained = p.records().await.iter().filter(|r| r.1 == "transition" && r.5 == b.atom && r.6.is_some()).count();
    assert_eq!(chained, 1);
    // Nothing left to judge: the next tick is idle and sends nothing.
    assert_eq!(judge::tick(&worker, &p.owner, &gated).await.unwrap().processed, 0);
    assert_eq!(model.calls.lock().unwrap().len(), 1);
    p.done().await;
}

// ── The extraction hook ─────────────────────────────────────────────────────

struct Extractor {
    outputs: Mutex<Vec<String>>,
}
#[async_trait::async_trait]
impl LlmProvider for Extractor {
    async fn complete(&self, req: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        assert_eq!(req.tool_schema.as_ref().unwrap().name, "emit_source_atoms_v2");
        Ok(CompletionResponse {
            content: self.outputs.lock().unwrap().remove(0),
            input_tokens: 10,
            output_tokens: 10,
            model: "mock-source-extractor".into(),
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
            stop_reason: Some("tool_use".into()),
            cost_usd: Some(0.00042),
        })
    }
    async fn stream_complete(
        &self,
        _: CompletionRequest,
    ) -> Result<std::pin::Pin<Box<dyn futures::Stream<Item = Result<StreamEvent, LlmError>> + Send>>, LlmError> {
        panic!("forced nonstreaming tool only")
    }
}

async fn capture_v2(p: &Plane, id: &str, text: &str) -> Uuid {
    let mut tx = begin_account_scoped(&p.app, p.account).await.unwrap();
    let capture = CaptureSource {
        source_type: "linear_issue",
        provider_namespace: "composio:linear",
        source_id: id,
        normalized_text: text,
        representation: "normalized_text",
        metadata: &json!({"title": id}),
        occurred_at: None,
        participants: &[],
        ingested_by: Some(p.user),
        scope: "private",
        duration_ms: None,
        segments: None,
    };
    let saved = source_evidence::capture_source_version_tx(&mut tx, None, p.graph, p.account, &capture, None, &[42; 32]).await.unwrap();
    tx.commit().await.unwrap();
    // A capture pins the active profile, v2 by default: no repin.
    let pin: String = sqlx::query_scalar("SELECT extraction_version FROM source_versions WHERE version_id=$1")
        .bind(saved.version_id)
        .fetch_one(&p.owner)
        .await
        .unwrap();
    assert_eq!(pin, "source-atoms-v2");
    saved.version_id
}

async fn extract(p: &Plane, model: &Extractor, version: Uuid) {
    let (_, account, created) = support::managed_extraction::own_version(&p.owner, version).await;
    if created {
        let mut tx = begin_account_scoped(&p.owner, account).await.unwrap();
        source_evidence::queue_extraction_tx(&mut tx, p.graph, version, account, 0).await.unwrap();
        tx.commit().await.unwrap();
    }
    let row: (Uuid, String, Value) = sqlx::query_as(
        "SELECT id,kind,payload FROM jobs WHERE payload->>'source_version_id'=$1 AND status='pending' ORDER BY created_at DESC LIMIT 1",
    )
    .bind(version.to_string())
    .fetch_one(&p.owner)
    .await
    .unwrap();
    source_atoms::process_job(
        &TenantId::Consumer,
        &p.app,
        &p.owner,
        &PostgresStore::new(p.app.clone()),
        &coralswarm_backend::llm::testing::GatedScript(model),
        None,
        JobRow { id: row.0, kind: row.1, payload: row.2, attempts: 0, account_id: Some(account) },
    )
    .await;
    let status: String = sqlx::query_scalar("SELECT extraction_status FROM source_versions WHERE version_id=$1")
        .bind(version)
        .fetch_one(&p.owner)
        .await
        .unwrap();
    assert_eq!(status, "done");
}

fn v2_atom(text: &str, owner: &str, from: Option<&str>, until: Option<&str>, quote: &str) -> String {
    let rel = json!({"subject": owner, "predicate": "responsible_for", "object": "Apollo Platform",
        "valid_from": from, "valid_until": until, "validity_quote": quote});
    json!({"atoms":[{"kind":"fact","text":text,"evidence_quote":text,
        "entities":[{"mention":owner,"type":"person"},{"mention":"Apollo Platform","type":"project"}],
        "relations":[rel],"valid_from":from,"valid_until":until,"validity_quote":quote}]})
    .to_string()
}

/// The real extraction path: a v2 window that finishes a version records
/// its temporal evidence and decides supersession in the same transaction,
/// as the worker's app role. With the mode off it records nothing.
#[tokio::test]
async fn extraction_v2_records_temporal_evidence_and_supersession_on_its_last_window() {
    let _lock = ENV.lock().await;
    let Some(opts) = base() else { return };
    let p = Plane::consumer(&opts).await;
    let t1 = "Xavier Ortiz owns Apollo Platform until 2026-03-01.";
    let t2 = "Zoe Park owns Apollo Platform from 2026-04-01.";
    let model = Extractor {
        outputs: Mutex::new(vec![
            v2_atom(t1, "Xavier Ortiz", None, Some("2026-03-01"), "until 2026-03-01"),
            v2_atom(t2, "Zoe Park", Some("2026-04-01"), None, "from 2026-04-01"),
            v2_atom("Wen Li owns Apollo Platform from 2026-05-01.", "Wen Li", Some("2026-05-01"), None, "from 2026-05-01"),
        ]),
    };
    let first = capture_v2(&p, "own-1", t1).await;
    let second = capture_v2(&p, "own-2", t2).await;
    {
        let _g = mode("live");
        extract(&p, &model, first).await;
        extract(&p, &model, second).await;
    }
    let apollo: Vec<Uuid> = sqlx::query_scalar("SELECT node_id FROM nodes WHERE graph_id=$1 AND title='Apollo Platform'")
        .bind(p.graph)
        .fetch_all(&p.owner)
        .await
        .unwrap();
    let records = p.records().await;
    let owner: Vec<_> = records.iter().filter(|r| r.0 == "rel.owner").map(|r| (r.1.as_str(), r.2.as_deref())).collect();
    assert!(owner.contains(&("assert", Some("Xavier Ortiz"))) && owner.contains(&("withdraw", None)), "{owner:?}");
    assert!(owner.contains(&("assert", Some("Zoe Park"))), "{owner:?}");
    if apollo.len() == 1 {
        // Both versions bound the same Apollo reef: the replacement is decided
        // and chained on the extraction path itself.
        assert!(owner.contains(&("transition", Some("Zoe Park"))), "{owner:?}");
        let states: Vec<_> = p.edges().await.into_iter().filter(|e| e.0 == "rel.owner").map(|e| (e.3, e.4)).collect();
        assert_eq!(states, vec![("explicit_end".to_string(), "active".to_string())]);
    } else {
        panic!("extraction minted {} Apollo reefs; supersession needs one subject", apollo.len());
    }

    // Off: a third version extracts normally and records nothing.
    let third = capture_v2(&p, "own-3", "Wen Li owns Apollo Platform from 2026-05-01.").await;
    let before = (p.records().await.len(), p.edges().await.len());
    {
        let _g = mode("off");
        extract(&p, &model, third).await;
    }
    assert_eq!((p.records().await.len(), p.edges().await.len()), before);
    let atoms: i64 = p.count("SELECT count(*) FROM semantic_atoms WHERE graph_id=$1").await;
    assert_eq!(atoms, 3, "extraction itself is unaffected by the mode");
    p.done().await;
}
