//! #1545: team-seat history floors on every member read surface, through the
//! one boundary (`crate::history_floor`), under BOTH runtimes.
//!
//! - The OWNER pool is the production runtime: tenant requests connect as the
//!   table owner, RLS is not applied, and only the code-level floor can hold.
//!   This is the runtime that proves the fix.
//! - The least-privilege `coralswarm_tenant_app` pool is the defence-in-depth
//!   runtime: RLS applies (0064 windows legacy atoms only), and every surface
//!   must give the same answer through it.
//!
//! Four members read one team ocean, whose content sits an hour and a half
//! before and after `cutoff`:
//! - `full`: no grant (a legacy seat), full history: the positive control.
//! - `floored`: one grant with `history_from = cutoff`.
//! - `multi`: a floored grant AND a grant with no floor. The unfloored grant
//!   wins, so this member has full history.
//! - `minned`: grants floored at `cutoff` and at `cutoff + 2h`. The MIN wins,
//!   so this member reads from `cutoff`: post-floor content stays visible.
//!
//! Every surface asserts that `floored` and `minned` never see the pre-floor
//! row and still see the post-floor one, and that `full` and `multi` see both.
//!
//!   ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:15544/postgres \
//!     cargo test -p coralswarm-backend --features test-support \
//!       --test history_floor_surfaces_pg -- --test-threads=1

mod support;

use std::{str::FromStr, sync::Arc};

use axum::{body::to_bytes, extract::{Query, State}, Extension};
use chrono::{DateTime, Duration, DurationRound, Utc};
use coralswarm_backend::{
    library_read::{self, LibraryJson, LibraryScope, SearchRequest},
    source_evidence,
    store::{PostgresStore, Store},
    tenancy::{
        extractor::Tenant,
        runtime::{TenancyDecision, TenantContext},
        scope::begin_tenant_scoped,
    },
    UserContext,
};
use pgvector::Vector;
use serde_json::{json, Value};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    Connection, Executor, PgConnection, PgPool,
};
use support::tenant_role::{set_app_role_password_with_retry, APP_ROLE_TEST_PASSWORD};
use uuid::Uuid;

const ORG: &str = "org_history_floor_surfaces";

#[derive(Clone, Copy, Debug, PartialEq)]
enum Runtime {
    Owner,
    App,
}

#[derive(Clone, Copy, Debug)]
struct Member {
    id: Uuid,
    /// Whether this member must be floored at `cutoff`.
    floored: bool,
    label: &'static str,
}

struct Fx {
    admin: PgConnection,
    name: coralswarm_backend::test_db::TestDatabase,
    owner: PgPool,
    app: PgPool,
    account: Uuid,
    graph: Uuid,
    members: [Member; 4],
    cutoff: DateTime<Utc>,
    pre: DateTime<Utc>,
    post: DateTime<Utc>,
}

fn probe_vector() -> Vec<f32> {
    (0..384).map(|i| (i as f32 * 0.01).sin()).collect()
}

impl Fx {
    async fn setup(suffix: &str) -> Option<Self> {
        let Ok(base) = std::env::var("ORGT_TEST_BASE_URL") else {
            assert!(std::env::var("REQUIRE_PG_SUITE").is_err(), "REQUIRE_PG_SUITE set without ORGT_TEST_BASE_URL");
            eprintln!("skipping history_floor_surfaces_pg: set ORGT_TEST_BASE_URL");
            return None;
        };
        let opts = PgConnectOptions::from_str(&base).unwrap();
        assert!(matches!(opts.get_host(), "localhost" | "127.0.0.1" | "::1"));
        let mut admin = PgConnection::connect_with(&opts).await.unwrap();
        let name = coralswarm_backend::test_db::migrated_database(
            &opts,
            coralswarm_backend::test_db::Plane::Tenant,
            &format!("history_floor_surfaces_{suffix}"),
        )
        .await;
        let owner = PgPoolOptions::new().max_connections(4).connect_with(opts.clone().database(&name)).await.unwrap();
        let bypass: bool = sqlx::query_scalar(
            "SELECT r.rolsuper OR r.rolbypassrls OR c.relowner=r.oid AND NOT c.relforcerowsecurity \
             FROM pg_roles r, pg_class c WHERE r.rolname=current_user AND c.oid='public.atoms'::regclass",
        )
        .fetch_one(&owner)
        .await
        .unwrap();
        assert!(bypass, "the owner pool must bypass RLS, like production");
        set_app_role_password_with_retry(&owner).await;
        let app = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(opts.database(&name).username("coralswarm_tenant_app").password(APP_ROLE_TEST_PASSWORD))
            .await
            .unwrap();
        let app_bypass: bool = sqlx::query_scalar("SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname=current_user")
            .fetch_one(&app)
            .await
            .unwrap();
        assert!(!app_bypass, "the app pool must be the least-privilege role");
        sqlx::query("INSERT INTO tenant_meta(id,org_id) VALUES(true,$1)").bind(ORG).execute(&owner).await.unwrap();

        let account = Uuid::new_v4();
        let cutoff = (Utc::now() - Duration::days(30)).duration_trunc(Duration::hours(1)).unwrap();
        let members = [
            Member { id: Uuid::new_v4(), floored: false, label: "full" },
            Member { id: Uuid::new_v4(), floored: true, label: "floored" },
            Member { id: Uuid::new_v4(), floored: false, label: "multi (one unfloored grant)" },
            Member { id: Uuid::new_v4(), floored: true, label: "minned (MIN of two floors)" },
        ];
        let graph: Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind) VALUES($1,'Team ocean','team') RETURNING id")
            .bind(account).fetch_one(&owner).await.unwrap();
        let team: Uuid = sqlx::query_scalar("INSERT INTO teams(account_id,graph_id,name,created_by) VALUES($1,$2,'Eng',$3) RETURNING id")
            .bind(account).bind(graph).bind(members[0].id).fetch_one(&owner).await.unwrap();
        for m in &members {
            sqlx::query("INSERT INTO team_members(team_id,user_id,role) VALUES($1,$2,'member')").bind(team).bind(m.id).execute(&owner).await.unwrap();
        }
        let grant = |user: Uuid, source: &'static str, from: Option<DateTime<Utc>>| {
            let owner = owner.clone();
            async move {
                sqlx::query("INSERT INTO team_seat_grants(account_id,team_id,user_id,source,history_from) VALUES($1,$2,$3,$4,$5)")
                    .bind(account).bind(team).bind(user).bind(source).bind(from).execute(&owner).await.unwrap();
            }
        };
        grant(members[1].id, "slack:C1", Some(cutoff)).await;
        grant(members[2].id, "slack:C1", Some(cutoff)).await;
        grant(members[2].id, "teams:T1", None).await;
        grant(members[3].id, "slack:C1", Some(cutoff + Duration::hours(2))).await;
        grant(members[3].id, "teams:T1", Some(cutoff)).await;
        let (pre, post) = (cutoff - Duration::minutes(90), cutoff + Duration::minutes(90));
        Some(Self { admin, name, owner, app, account, graph, members, cutoff, pre, post })
    }

    fn pool(&self, rt: Runtime) -> &PgPool {
        match rt {
            Runtime::Owner => &self.owner,
            Runtime::App => &self.app,
        }
    }

    fn tenant(&self, rt: Runtime, user: Uuid) -> Tenant {
        let pool = self.pool(rt).clone();
        Tenant {
            pool: Some(pool.clone()),
            store: Some(Arc::new(PostgresStore::new(pool.clone()))),
            decision: Some(TenancyDecision::Org(TenantContext {
                org_id: ORG.into(),
                account_id: self.account,
                user_id: user,
                pool,
                page_pool: None,
            })),
            class: coralswarm_backend::route_class::RouteClass::Other,
        }
    }

    fn user(&self, user: Uuid) -> UserContext {
        UserContext { user_id: user, account_id: self.account, role: "member".into(), device_id: "history-floor-surfaces".into() }
    }

    /// A legacy atom at `ts` (NULL for "no time"), optionally with an
    /// embedding and an ingesting member.
    async fn atom(&self, source_id: &str, ts: Option<DateTime<Utc>>, embed: bool, ingested_by: Option<Uuid>) -> Uuid {
        let v = embed.then(|| Vector::from(probe_vector()));
        sqlx::query_scalar(
            "INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,content_tsv,embedding,embed_model,embedding_next,embed_model_next,\"timestamp\",ingested_by,title) \
             VALUES($1,'note',$2,'blob',$3,to_tsvector('simple',$3),$4,$5,$4,$6,$7,$8,$3) RETURNING atom_id",
        )
        .bind(self.graph).bind(source_id).bind(format!("wombat ledger {source_id}"))
        .bind(v).bind(coralswarm_core::embed::EMBED_MODEL_NAME).bind(coralswarm_core::embed::EMBED_MODEL_CLOUD).bind(ts).bind(ingested_by)
        .fetch_one(&self.owner).await.unwrap()
    }

    /// A permitted source item whose versions occurred at `moments`, the last
    /// one current. Returns (item, versions).
    async fn source(&self, tag: &str, moments: &[DateTime<Utc>]) -> (Uuid, Vec<Uuid>) {
        let id = format!("hfs-item-{tag}");
        let origin = json!({"service_id":"slack","transport":"native","content_kind":"message","connection":{"kind":"oauth_grant","id":"fixture"},"container":{"kind":"channel","id":"C1","provider_namespace":id}});
        let item: Uuid = sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,origin,access_state) VALUES($1,'slack',$2,$2,$3,'permitted') RETURNING source_item_id")
            .bind(self.graph).bind(&id).bind(&origin).fetch_one(&self.owner).await.unwrap();
        let mut versions = vec![];
        for (i, at) in moments.iter().enumerate() {
            // captured_at is after the floor for every version, so only the
            // version's occurred_at can decide.
            let version: Uuid = sqlx::query_scalar(
                "INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,origin,raw_inline,source_metadata,occurred_at,captured_at,extraction_status) \
                 VALUES($1,$2,$3,'slack',$3,$4,$5,$6,$7,now() + make_interval(secs => $8),'done') RETURNING version_id",
            )
            .bind(self.graph).bind(item).bind(&id).bind(&origin).bind(format!("wombat ledger raw text {tag} v{i}"))
            .bind(json!({"title":format!("Wombat capture {tag} v{i}")})).bind(at).bind(i as f64)
            .fetch_one(&self.owner).await.unwrap();
            versions.push(version);
        }
        sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1").bind(item).bind(versions.last().unwrap()).execute(&self.owner).await.unwrap();
        (item, versions)
    }

    async fn teardown(mut self) {
        self.owner.close().await;
        self.app.close().await;
        self.admin.execute(format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", self.name).as_str()).await.ok();
    }
}

/// `seen` holds (member, ids) for one surface under one runtime.
fn assert_floor<T: PartialEq + std::fmt::Debug>(surface: &str, rt: Runtime, seen: &[(Member, Vec<T>)], pre: &T, post: &T) {
    for (m, got) in seen {
        assert!(got.contains(post), "{surface} [{rt:?}]: {} must still see the post-floor row, got {got:?}", m.label);
        if m.floored {
            assert!(!got.contains(pre), "{surface} [{rt:?}]: {} saw a pre-floor row: {got:?}", m.label);
        } else {
            assert!(got.contains(pre), "{surface} [{rt:?}]: {} has full history and must see the pre-floor row, got {got:?}", m.label);
        }
    }
}

fn ids(v: &Value, pointer: &str, key: &str) -> Vec<Uuid> {
    v.pointer(pointer)
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("{pointer} missing in {v}"))
        .iter()
        .filter_map(|e| e[key].as_str().and_then(|s| Uuid::parse_str(s).ok()))
        .collect()
}

/// The issue's owner-role test gaps: several grants where one has no floor,
/// the MIN across floors, and an atom with no time; through the atom
/// boundary and the new self-contained SQL predicates.
#[tokio::test]
async fn floor_resolution_multi_grant_min_and_no_time() {
    let Some(fx) = Fx::setup("resolution").await else { return };
    let pre = fx.atom("hfs-res-pre", Some(fx.pre), false, None).await;
    let post = fx.atom("hfs-res-post", Some(fx.post), false, None).await;
    // "An atom with no time". `atoms."timestamp"` is NOT NULL, so a legacy
    // atom always has one. A semantic atom's version may have no
    // occurred_at: its moment falls back to captured_at. Two such outcomes,
    // captured before and after the floor.
    let mut untimed = vec![];
    for (tag, captured) in [("pre", fx.pre), ("post", fx.post)] {
        let id = format!("hfs-untimed-{tag}");
        let item: Uuid = sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,access_state) VALUES($1,'slack',$2,$2,'permitted') RETURNING source_item_id")
            .bind(fx.graph).bind(&id).fetch_one(&fx.owner).await.unwrap();
        let version: Uuid = sqlx::query_scalar(
            "INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,raw_inline,occurred_at,captured_at,extraction_status) \
             VALUES($1,$2,$3,'slack',$3,'untimed',NULL,$4,'done') RETURNING version_id",
        )
        .bind(fx.graph).bind(item).bind(&id).bind(captured).fetch_one(&fx.owner).await.unwrap();
        sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1").bind(item).bind(version).execute(&fx.owner).await.unwrap();
        untimed.push(
            sqlx::query_scalar::<_, Uuid>("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model,entities) VALUES($1,$2,0,'fact','untimed','source-atoms-v1','fixture','[]') RETURNING semantic_atom_id")
                .bind(fx.graph).bind(version).fetch_one(&fx.owner).await.unwrap(),
        );
    }
    for rt in [Runtime::Owner, Runtime::App] {
        let mut permitted = vec![];
        let mut served = vec![];
        for m in fx.members {
            let mut tx = begin_tenant_scoped(fx.pool(rt), fx.account, m.id).await.unwrap();
            let floors: Option<Value> = sqlx::query_scalar("SELECT public.reader_history_floors()").fetch_one(&mut *tx).await.unwrap();
            let floor = floors.as_ref().and_then(|f| f[fx.graph.to_string()].as_str().map(|s| DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)));
            if m.floored {
                assert_eq!(floor, Some(fx.cutoff), "[{rt:?}] {}: the floor is the MIN of the member's floors: {floors:?}", m.label);
            } else {
                assert_eq!(floors, None, "[{rt:?}] {}: an unfloored grant absorbs every floor: {floors:?}", m.label);
            }
            let got: Vec<Uuid> = sqlx::query_scalar("SELECT atom_id FROM library_permitted_atoms($1,NULL)").bind(fx.graph).fetch_all(&mut *tx).await.unwrap();
            // A NULL moment is hidden under a floor and visible without one.
            let null_moment: bool = sqlx::query_scalar("SELECT public.history_floor_admits($1,NULL,(SELECT public.reader_history_floors()))")
                .bind(fx.graph).fetch_one(&mut *tx).await.unwrap();
            assert_eq!(null_moment, !m.floored, "[{rt:?}] {}: a NULL moment", m.label);
            let outcomes: Vec<Uuid> = sqlx::query_scalar("SELECT semantic_atom_id FROM served_semantic_atoms WHERE graph_id=$1")
                .bind(fx.graph).fetch_all(&mut *tx).await.unwrap();
            tx.rollback().await.unwrap();
            permitted.push((m, got));
            served.push((m, outcomes));
        }
        assert_floor("library_permitted_atoms", rt, &permitted, &pre, &post);
        assert_floor("served_semantic_atoms, version with no occurred_at (captured_at decides)", rt, &served, &untimed[0], &untimed[1]);
    }
    fx.teardown().await;
}

/// Item 1: source items, versions and raw source text (Library source list,
/// `/sources/items`, source detail and its versions, the version loader).
#[tokio::test]
async fn source_items_versions_and_raw_text_respect_the_floor() {
    let Some(fx) = Fx::setup("sources").await else { return };
    let (item_pre, _) = fx.source("pre", &[fx.pre]).await;
    let (item_post, _) = fx.source("post", &[fx.post]).await;
    // One item captured before the floor and again after it: the item is
    // shown at its current, post-floor version; the old version is not.
    let (item_mixed, mixed) = fx.source("mixed", &[fx.pre, fx.post]).await;
    let (v_old, v_new) = (mixed[0], mixed[1]);

    // CONTROL plane for the source-detail admission re-check.
    let base = std::env::var("ORGT_TEST_BASE_URL").unwrap();
    let opts = PgConnectOptions::from_str(&base).unwrap();
    let mut admin = PgConnection::connect_with(&opts).await.unwrap();
    let control_name = coralswarm_backend::test_db::migrated_database(
        &opts,
        coralswarm_backend::test_db::Plane::Control,
        "history_floor_surfaces_control",
    )
    .await;
    let control = PgPoolOptions::new().max_connections(4).connect_with(opts.database(&control_name)).await.unwrap();
    for m in fx.members {
        sqlx::query("INSERT INTO users(id,email) VALUES($1,$2)").bind(m.id).bind(format!("{}@floor.example.test", m.id)).execute(&control).await.unwrap();
    }
    sqlx::query("INSERT INTO accounts(id,owner_user_id,clerk_org_id) VALUES($1,$2,$3)").bind(fx.account).bind(fx.members[0].id).bind(ORG).execute(&control).await.unwrap();
    for m in fx.members {
        sqlx::query("INSERT INTO account_members(account_id,user_id,role,seat_tier_id) VALUES($1,$2,'member',(SELECT id FROM seat_tiers WHERE code='standard' AND is_active))")
            .bind(fx.account).bind(m.id).execute(&control).await.unwrap();
    }
    let state = Arc::new(coralswarm_backend::test_app_state_owned_with_db(control.clone(), None));

    for rt in [Runtime::Owner, Runtime::App] {
        let (mut inventory, mut library, mut loader, mut detail, mut versions, mut raw) = (vec![], vec![], vec![], vec![], vec![], vec![]);
        for m in fx.members {
            let (status, axum::Json(items)) = source_evidence::list_items(
                fx.tenant(rt, m.id),
                Extension(fx.user(m.id)),
                Query(source_evidence::ItemsQuery { graph_id: fx.graph, before: None, source_type: None, atom_id: None }),
            )
            .await;
            assert_eq!(status, 200, "{items}");
            let listed = ids(&items, "/items", "source_item_id");
            assert!(listed.contains(&item_mixed), "[{rt:?}] {}: the re-captured item is shown at its post-floor version: {items}", m.label);
            inventory.push((m, listed));

            let mut scope: LibraryScope = serde_json::from_value(json!({"graph_ids":[fx.graph]})).unwrap();
            scope.normalize().unwrap();
            let q = SearchRequest { scope, query: String::new(), page_size: Some(40), sources_after: None, outcomes_after: None, nodes_after: None };
            let response = library_read::search(fx.tenant(rt, m.id), Extension(fx.user(m.id)), LibraryJson(q)).await;
            assert_eq!(response.status(), 200);
            let v: Value = serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
            let titles: Vec<String> = v["sources"]["items"].as_array().unwrap().iter().filter_map(|s| s["title"].as_str().map(str::to_owned)).collect();
            if m.floored {
                assert!(!titles.iter().any(|t| t.contains(" pre ") || t.ends_with("pre v0")), "[{rt:?}] {}: a pre-floor title was listed: {titles:?}", m.label);
            }
            library.push((m, ids(&v, "/sources/items", "source_item_id")));

            let mut tx = begin_tenant_scoped(fx.pool(rt), fx.account, m.id).await.unwrap();
            let mut opened = vec![];
            for version in [v_old, v_new] {
                if source_evidence::load_version_scoped(&mut tx, fx.graph, version).await.unwrap().is_some() {
                    opened.push(version);
                }
            }
            tx.rollback().await.unwrap();
            loader.push((m, opened));

            let mut found = vec![];
            let mut texts = vec![];
            for item in [item_pre, item_post] {
                let (status, axum::Json(body)) = source_evidence::get_evidence(
                    State(state.clone()),
                    fx.tenant(rt, m.id),
                    Extension(fx.user(m.id)),
                    Query(source_evidence::EvidenceQuery { graph_id: fx.graph, source_item_id: item, version_id: None, before: None, atoms_after: None, atom_id: None }),
                )
                .await;
                if status == 200 {
                    found.push(item);
                    texts.push(body["selected_version"]["raw_text"].as_str().unwrap_or_default().to_owned());
                } else {
                    assert_eq!(status, 404, "[{rt:?}] {}: {body}", m.label);
                }
            }
            detail.push((m, found));
            raw.push((m, texts));

            let (status, axum::Json(body)) = source_evidence::get_evidence(
                State(state.clone()),
                fx.tenant(rt, m.id),
                Extension(fx.user(m.id)),
                Query(source_evidence::EvidenceQuery { graph_id: fx.graph, source_item_id: item_mixed, version_id: None, before: None, atoms_after: None, atom_id: None }),
            )
            .await;
            assert_eq!(status, 200, "[{rt:?}] {}: {body}", m.label);
            let mut listed = ids(&body, "/versions", "version_id");
            let (status, axum::Json(old)) = source_evidence::get_evidence(
                State(state.clone()),
                fx.tenant(rt, m.id),
                Extension(fx.user(m.id)),
                Query(source_evidence::EvidenceQuery { graph_id: fx.graph, source_item_id: item_mixed, version_id: Some(v_old), before: None, atoms_after: None, atom_id: None }),
            )
            .await;
            if m.floored {
                assert_eq!(status, 404, "[{rt:?}] {}: the pre-floor version must not open: {old}", m.label);
            } else {
                assert_eq!(status, 200, "[{rt:?}] {}: {old}", m.label);
                assert_eq!(old["selected_version"]["raw_text"], "wombat ledger raw text mixed v0", "{old}");
            }
            listed.sort();
            versions.push((m, listed));
        }
        assert_floor("/sources/items", rt, &inventory, &item_pre, &item_post);
        assert_floor("Library search sources", rt, &library, &item_pre, &item_post);
        assert_floor("load_version_scoped", rt, &loader, &v_old, &v_new);
        assert_floor("source detail (GET /sources/evidence)", rt, &detail, &item_pre, &item_post);
        assert_floor("source detail versions", rt, &versions, &v_old, &v_new);
        assert_floor("source detail raw text", rt, &raw, &"wombat ledger raw text pre v0".to_owned(), &"wombat ledger raw text post v0".to_owned());
    }
    control.close().await;
    admin.execute(format!("DROP DATABASE IF EXISTS {control_name} WITH (FORCE)").as_str()).await.ok();
    fx.teardown().await;
}

/// Item 2: meeting transcripts: the list, the raw-text read, the summary
/// state and the meeting-derived atom reads. A transcript's moment is its
/// EARLIEST atom: "mtg-pre" was written after the floor but holds a
/// pre-floor atom (and a post-floor one, which must not lift it).
#[tokio::test]
async fn meeting_transcripts_respect_the_floor() {
    let Some(fx) = Fx::setup("meetings").await else { return };
    for (meeting, ts) in [("mtg-pre", Some(fx.pre)), ("mtg-post", Some(fx.post))] {
        sqlx::query("INSERT INTO meeting_transcripts(graph_id,source_id,raw_inline) VALUES($1,$2,$3)")
            .bind(fx.graph).bind(meeting).bind(format!("wombat transcript {meeting}")).execute(&fx.owner).await.unwrap();
        fx.atom(&format!("{meeting}:chunk:0"), ts, false, None).await;
        // A later chunk after the floor must not lift an earlier one.
        fx.atom(&format!("{meeting}:chunk:1"), Some(fx.post), false, None).await;
    }
    let store = |rt: Runtime| PostgresStore::new(fx.pool(rt).clone());
    let (pre, post) = ("mtg-pre".to_owned(), "mtg-post".to_owned());
    for rt in [Runtime::Owner, Runtime::App] {
        let s = store(rt);
        let (mut listed, mut fetched, mut status, mut occurred, mut routed) = (vec![], vec![], vec![], vec![], vec![]);
        for m in fx.members {
            let mut tx = begin_tenant_scoped(fx.pool(rt), fx.account, m.id).await.unwrap();
            let list: Vec<String> = s.list_meeting_transcripts_scoped(&mut tx, fx.graph, 50, 0, None, None).await.unwrap().into_iter().map(|t| t.source_id).collect();
            listed.push((m, list));
            let mut got = vec![];
            let mut states = vec![];
            let mut at = vec![];
            for meeting in [&pre, &post] {
                if let Some(t) = s.get_meeting_transcript_scoped(&mut tx, fx.graph, meeting).await.unwrap() {
                    assert_eq!(t.raw_inline.as_deref(), Some(format!("wombat transcript {meeting}").as_str()));
                    got.push(meeting.clone());
                }
                if s.meeting_summary_status_scoped(&mut tx, fx.graph, meeting).await.unwrap().is_some() {
                    states.push(meeting.clone());
                }
                // Listed when the reported time is the meeting's first
                // seeded atom: a floored member's never is for "mtg-pre",
                // whose first atom is before the floor.
                let first = if meeting == &pre { fx.pre } else { fx.post };
                if s.meeting_occurred_at_scoped(&mut tx, fx.graph, meeting).await.unwrap() == Some(first) {
                    at.push(meeting.clone());
                }
            }
            tx.rollback().await.unwrap();
            fetched.push((m, got));
            status.push((m, states));
            occurred.push((m, at));

            let (code, axum::Json(body)) = coralswarm_backend::test_list_meetings(
                fx.tenant(rt, m.id),
                Extension(fx.user(m.id)),
                Query(coralswarm_backend::TestListMeetingsQuery { graph_id: fx.graph, limit: Some(50), cursor: None }),
            )
            .await;
            assert_eq!(code, 200, "{body}");
            let names: Vec<String> = body["items"].as_array().unwrap().iter().map(|i| i["source_id"].as_str().unwrap().to_owned()).collect();
            routed.push((m, names));
        }
        assert_floor("list_meeting_transcripts_scoped (MCP list_meetings)", rt, &listed, &pre, &post);
        assert_floor("get_meeting_transcript_scoped (raw text: get_meeting, get_transcript, shares)", rt, &fetched, &pre, &post);
        assert_floor("meeting_summary_status_scoped", rt, &status, &pre, &post);
        assert_floor("meeting_occurred_at_scoped", rt, &occurred, &pre, &post);
        assert_floor("GET /v1/meetings (list_meetings)", rt, &routed, &pre, &post);
    }
    fx.teardown().await;
}

/// Item 3: journal reads: the live hour aggregate, the hour's items and
/// totals, and the precomputed rollups.
#[tokio::test]
async fn journal_reads_respect_the_floor() {
    let Some(fx) = Fx::setup("journal").await else { return };
    let hour = |t: DateTime<Utc>| t.duration_trunc(Duration::hours(1)).unwrap();
    let (pre_hour, post_hour) = (hour(fx.pre), hour(fx.post));
    assert!(pre_hour < fx.cutoff && post_hour >= fx.cutoff);
    let mut atoms = std::collections::HashMap::new();
    for m in fx.members {
        let a = fx.atom(&format!("journal-{}-pre", m.id), Some(fx.pre), false, Some(m.id)).await;
        let b = fx.atom(&format!("journal-{}-post", m.id), Some(fx.post), false, Some(m.id)).await;
        atoms.insert(m.id, (a, b));
        for h in [pre_hour, post_hour] {
            sqlx::query("INSERT INTO journal_rollups(account_id,user_id,graph_id,hour_start,period_start,summary,input_fingerprint,model) VALUES($1,$2,$3,$4,$4,'wombat hour','fp','fixture')")
                .bind(fx.account).bind(m.id).bind(fx.graph).bind(h).execute(&fx.owner).await.unwrap();
        }
    }
    // Today gist (0218): one team gist per hour-long period, read back
    // through `services::journal_gist::read_gist` below.
    for h in [pre_hour, post_hour] {
        sqlx::query("INSERT INTO journal_gists(account_id,graph_id,scope,granularity,period_start,period_end,tz,gist,input_fingerprint,model) VALUES($1,$2,'team','day',$3,$4,'UTC',$5,'fp','fixture')")
            .bind(fx.account).bind(fx.graph).bind(h).bind(h + Duration::hours(1)).bind(serde_json::json!({"done": ["wombat"]}))
            .execute(&fx.owner).await.unwrap();
    }
    let (from, to) = (fx.cutoff - Duration::days(1), fx.cutoff + Duration::days(1));
    for rt in [Runtime::Owner, Runtime::App] {
        let s = PostgresStore::new(fx.pool(rt).clone());
        let (mut aggregate, mut items, mut totals, mut rollups) = (vec![], vec![], vec![], vec![]);
        let mut gists = vec![];
        for m in fx.members {
            let (a_pre, a_post) = atoms[&m.id];
            let mut tx = begin_tenant_scoped(fx.pool(rt), fx.account, m.id).await.unwrap();
            let hours: Vec<DateTime<Utc>> = s.journal_range_aggregate(&mut tx, m.id, &[fx.graph], from, to).await.unwrap().into_iter().filter(|h| h.atom_count > 0).map(|h| h.hour_start).collect();
            aggregate.push((m, hours));
            let mut seen = vec![];
            let mut counted = vec![];
            for h in [pre_hour, post_hour] {
                seen.extend(s.journal_hour_items(&mut tx, m.id, &[fx.graph], h, None, 50).await.unwrap().into_iter().map(|i| i.id));
                if s.journal_hour_item_totals(&mut tx, m.id, &[fx.graph], h).await.unwrap().atoms > 0 {
                    counted.push(h);
                }
            }
            items.push((m, seen.into_iter().filter(|id| *id == a_pre || *id == a_post).map(|id| if id == a_pre { "pre" } else { "post" }).collect::<Vec<_>>()));
            totals.push((m, counted));
            let r: Vec<DateTime<Utc>> = s.list_journal_rollups(&mut tx, m.id, &[fx.graph], "hour", from, to).await.unwrap().into_iter().map(|r| r.hour_start).collect();
            rollups.push((m, r));
            let mut shown = vec![];
            for h in [pre_hour, post_hour] {
                use coralswarm_backend::services::journal_gist as jg;
                let graphs = [fx.graph];
                let req = jg::ReadRequest {
                    caller: m.id,
                    who: jg::Who::Team,
                    granularity: jg::Granularity::Day,
                    period_start: h,
                    period_end: h + Duration::hours(1),
                    graph_ids: &graphs,
                };
                if jg::read_gist(&mut tx, &req).await.unwrap().gist.is_some() {
                    shown.push(h);
                }
            }
            gists.push((m, shown));
            tx.rollback().await.unwrap();
        }
        assert_floor("journal_range_aggregate (GET /v1/journal)", rt, &aggregate, &pre_hour, &post_hour);
        assert_floor("journal_hour_items (GET /v1/journal/hour/{h}/items)", rt, &items, &"pre", &"post");
        assert_floor("journal_hour_item_totals", rt, &totals, &pre_hour, &post_hour);
        assert_floor("list_journal_rollups", rt, &rollups, &pre_hour, &post_hour);
        assert_floor("read_gist (GET /v1/journal/gist)", rt, &gists, &pre_hour, &post_hour);
    }
    fx.teardown().await;
}

/// Item 4: store reads outside the permission function.
#[tokio::test]
async fn store_reads_outside_the_permission_function_respect_the_floor() {
    let Some(fx) = Fx::setup("store").await else { return };
    let pre = fx.atom("cc:sess-wombat:ck-pre", Some(fx.pre), true, None).await;
    let post = fx.atom("cc:sess-wombat:ck-post", Some(fx.post), true, None).await;
    for rt in [Runtime::Owner, Runtime::App] {
        let s = PostgresStore::new(fx.pool(rt).clone());
        let (mut candidates, mut corals) = (vec![], vec![]);
        for m in fx.members {
            let mut tx = begin_tenant_scoped(fx.pool(rt), fx.account, m.id).await.unwrap();
            candidates.push((m, s.node_candidates_scoped(&mut tx, fx.graph, &probe_vector(), &[], 10).await.unwrap().into_iter().map(|c| c.atom_id).collect::<Vec<_>>()));
            corals.push((m, s.list_shared_session_corals_scoped(&mut tx, fx.graph, "sess-wombat").await.unwrap().into_iter().map(|c| c.atom_id).collect::<Vec<_>>()));
            tx.rollback().await.unwrap();
        }
        assert_floor("node_candidates_scoped", rt, &candidates, &pre, &post);
        assert_floor("list_shared_session_corals_scoped", rt, &corals, &pre, &post);
    }
    fx.teardown().await;
}
