//! Live-Postgres proof of `GET /v1/journal/activities` and
//! `GET /v1/journal/activities/{key}` (`services::journal_activities`), driven
//! through the REAL production handlers over the `coralswarm_app` login (RLS
//! applies exactly as in production) with a hand-built `UserContext`, because
//! "two members of one account" is a shape a plain personal Clerk token cannot
//! produce (see `journal_endpoint_pg.rs`'s item 5).
//!
//! Fixture: one account with two members (A the owner, A2), one shared ocean
//! and one private (`member`) ocean per member.
//!
//!   1. Team shows every member's cards in the shared ocean and the caller's
//!      OWN private ocean, and NEVER the other member's private ocean (the
//!      secret title and its people appear nowhere in the answer, and the
//!      private ocean's key is a 404 in the detail route).
//!   2. Mine is only what the caller ingested; both members get the symmetric
//!      view (non-vacuous: each sees their own private card).
//!   3. Kind mapping and grouping: transcript chunks are one meeting card with
//!      its participants, the recorded duration and the stored summary's first
//!      sentence as `outcome`; a colon-bearing session groups once and takes the
//!      session title; an unmapped source type has no card.
//!   4. Enrichment: a decision-kind semantic atom counts as a decision and is
//!      the `outcome` fallback; the detail route returns decisions, a passage
//!      source labelled `NOTE · hh:mm`, and the reefs.
//!   5. Window validation, the flag (404 while off), `graph_id` narrowing, and
//!      the 14-day strip.
//!
//! Env-gated on `ORGT_TEST_BASE_URL`:
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test journal_activities_pg

mod support;

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use chrono::{DateTime, TimeZone, Utc};
use serde_json::{json, Value};
use sqlx::PgPool;
use tokio::sync::Mutex;
use uuid::Uuid;

use coralswarm_backend::store::PostgresStore;
use coralswarm_backend::tenancy::extractor::Tenant;
use coralswarm_backend::{
    journal_activities_list, journal_activity_detail, JournalActivitiesListParams,
    JournalActivityDetailParams, UserContext,
};
use support::rls_probe::RlsProbe;

/// Serializes the shared-process `TODAY_ACTIVITIES_ENABLED` mutations.
static ENV_LOCK: Mutex<()> = Mutex::const_new(());
const FLAG: &str = "TODAY_ACTIVITIES_ENABLED";

fn at(h: u32, m: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 29, h, m, 0).unwrap()
}

struct World {
    probe: RlsProbe,
    account: Uuid,
    a: Uuid,
    a2: Uuid,
    shared: Uuid,
    a_private: Uuid,
    a2_private: Uuid,
}

async fn user(owner: &PgPool, tag: &str, name: Option<&str>) -> Uuid {
    sqlx::query_scalar("INSERT INTO users(email, name) VALUES ($1, $2) RETURNING id")
        .bind(format!(
            "{tag}_{}@activities.example",
            Uuid::new_v4().simple()
        ))
        .bind(name)
        .fetch_one(owner)
        .await
        .expect("insert user")
}

async fn world(db: &str) -> Option<World> {
    let probe = RlsProbe::setup(db).await?;
    let o = &probe.owner;
    let a = user(o, "a", Some("Priya Nair")).await;
    let a2 = user(o, "a2", Some("Tom Becker")).await;
    let account: Uuid =
        sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
            .bind(a)
            .fetch_one(o)
            .await
            .unwrap();
    for (u, role) in [(a, "owner"), (a2, "member")] {
        sqlx::query("INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, $3)")
            .bind(account)
            .bind(u)
            .bind(role)
            .execute(o)
            .await
            .unwrap();
    }
    let shared: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name) VALUES ($1, 'Platform') RETURNING id",
    )
    .bind(account)
    .fetch_one(o)
    .await
    .unwrap();
    let mut private = Vec::new();
    for (u, name) in [(a, "Private A"), (a2, "Private A2")] {
        private.push(
            sqlx::query_scalar::<_, Uuid>("INSERT INTO graphs (account_id, name, kind, owner_user_id) VALUES ($1, $2, 'member', $3) RETURNING id")
                .bind(account)
                .bind(name)
                .bind(u)
                .fetch_one(o)
                .await
                .unwrap(),
        );
    }
    Some(World {
        account,
        a,
        a2,
        shared,
        a_private: private[0],
        a2_private: private[1],
        probe,
    })
}

impl World {
    fn tenant(&self) -> Tenant {
        Tenant {
            store: Some(Arc::new(PostgresStore::new(self.probe.app_pool.clone()))),
            pool: Some(self.probe.app_pool.clone()),
            decision: None,
            class: coralswarm_backend::route_class::RouteClass::Other,
        }
    }

    fn state(&self) -> State<Arc<coralswarm_backend::AppState>> {
        let mut state: coralswarm_backend::AppState =
            (*coralswarm_backend::test_app_state()).clone();
        state.db = Some(self.probe.app_pool.clone());
        state.store = Some(Arc::new(PostgresStore::new(self.probe.app_pool.clone())));
        State(Arc::new(state))
    }

    fn ctx(&self, user_id: Uuid) -> UserContext {
        UserContext {
            user_id,
            account_id: self.account,
            role: "member".into(),
            device_id: "test".into(),
        }
    }

    async fn list(
        &self,
        user_id: Uuid,
        who: &str,
        extra: impl FnOnce(&mut JournalActivitiesListParams),
    ) -> (StatusCode, Value) {
        let mut params = JournalActivitiesListParams {
            from: Some("2026-10-29T00:00:00Z".into()),
            to: Some("2026-10-30T00:00:00Z".into()),
            who: Some(who.into()),
            graph_id: None,
            tz: Some("UTC".into()),
        };
        extra(&mut params);
        let query:std::collections::BTreeMap<String,String>=[
            ("from",params.from),("to",params.to),("who",params.who),
            ("graph_id",params.graph_id.map(|g|g.to_string())),("tz",params.tz),
        ].into_iter().filter_map(|(k,v)|v.map(|v|(k.to_owned(),v))).collect();
        let _ = support::read_models::warm::<coralswarm_backend::read_models::today::JournalActivities>(
            &self.probe.app_pool,self.account,user_id,query.clone()).await;
        let resp = journal_activities_list(self.state(),self.tenant(),axum::Extension(self.ctx(user_id)),None,Query(query)).await;
        let (status,body)=read(resp).await;
        (status,support::read_models::payload(body))
    }

    async fn detail(&self, user_id: Uuid, key: &str, graph: Uuid) -> (StatusCode, Value) {
        let resp = journal_activity_detail(
            self.state(),
            self.tenant(),
            axum::Extension(self.ctx(user_id)),
            Path(key.to_string()),
            Query(JournalActivityDetailParams {
                graph_id: graph,
                tz: Some("UTC".into()),
            }),
        )
        .await;
        read(resp).await
    }

    #[allow(clippy::too_many_arguments)]
    async fn atom(
        &self,
        graph: Uuid,
        by: Uuid,
        source_type: &str,
        source_id: &str,
        when: DateTime<Utc>,
        title: Option<&str>,
        participants: &[&str],
    ) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, ingested_by, \"timestamp\", title, participants, excerpt) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING atom_id",
        )
        .bind(graph)
        .bind(source_type)
        .bind(source_id)
        .bind(format!("blob-{source_id}"))
        .bind(by)
        .bind(when)
        .bind(title)
        .bind(participants.iter().map(|p| p.to_string()).collect::<Vec<_>>())
        .bind(format!("excerpt of {source_id}"))
        .fetch_one(&self.probe.owner)
        .await
        .expect("insert atom")
    }
}

async fn read(resp: axum::response::Response) -> (StatusCode, Value) {
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

fn titles(body: &Value) -> Vec<String> {
    body["activities"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["title"].as_str().unwrap().to_string())
        .collect()
}

fn find<'a>(body: &'a Value, title: &str) -> &'a Value {
    body["activities"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["title"] == title)
        .unwrap_or_else(|| panic!("no card titled {title:?} in {body}"))
}

#[tokio::test]
async fn team_never_leaks_another_members_private_ocean_and_mine_is_only_the_callers() {
    let _env = ENV_LOCK.lock().await;
    let Some(w) = world("orgt_activities_team").await else {
        eprintln!("skipping team_never_leaks_another_members_private_ocean_and_mine_is_only_the_callers: set ORGT_TEST_BASE_URL");
        return;
    };
    std::env::set_var(FLAG, "1");

    w.atom(
        w.shared,
        w.a,
        "note",
        "note-a",
        at(9, 0),
        Some("A shared note"),
        &[],
    )
    .await;
    w.atom(
        w.shared,
        w.a2,
        "note",
        "note-a2",
        at(10, 0),
        Some("A2 shared note"),
        &[],
    )
    .await;
    w.atom(
        w.a_private,
        w.a,
        "note",
        "note-a-priv",
        at(11, 0),
        Some("A private note"),
        &[],
    )
    .await;
    w.atom(
        w.a2_private,
        w.a2,
        "note",
        "note-a2-priv",
        at(12, 0),
        Some("SECRET A2 private note"),
        &["Secret Person"],
    )
    .await;
    // An atom A somehow ingested into A2's private ocean: A cannot read that
    // ocean, so it is in neither of A's views (it is A2's to see).
    w.atom(
        w.a2_private,
        w.a,
        "note",
        "note-a-in-a2-priv",
        at(13, 0),
        Some("SECRET A in A2 private"),
        &[],
    )
    .await;

    // Team, as A.
    let (status, team_a) = w.list(w.a, "team", |_| {}).await;
    assert_eq!(status, StatusCode::OK, "{team_a}");
    assert_eq!(
        titles(&team_a),
        vec!["A shared note", "A2 shared note", "A private note"],
        "{team_a}"
    );
    let leaked = team_a.to_string();
    assert!(
        !leaked.contains("SECRET") && !leaked.contains("Secret Person"),
        "another member's private ocean leaked: {leaked}"
    );
    assert!(!leaked.contains(&w.a2_private.to_string()), "{leaked}");
    // Names come from the control plane.
    assert_eq!(
        find(&team_a, "A shared note")["owner"]["name"],
        json!("Priya Nair")
    );
    assert_eq!(
        find(&team_a, "A2 shared note")["owner"]["name"],
        json!("Tom Becker")
    );
    assert_eq!(
        find(&team_a, "A2 shared note")["graph"],
        json!({"id": w.shared, "name": "Platform"})
    );

    // Team, as A2: symmetric (non-vacuous: A2 sees A2's own private card).
    let (_, team_a2) = w.list(w.a2, "team", |_| {}).await;
    assert_eq!(
        titles(&team_a2),
        vec![
            "A shared note",
            "A2 shared note",
            "SECRET A2 private note",
            "SECRET A in A2 private"
        ],
        "{team_a2}"
    );
    assert!(!team_a2.to_string().contains("A private note"), "{team_a2}");

    // Mine.
    let (_, mine_a) = w.list(w.a, "mine", |_| {}).await;
    assert_eq!(
        titles(&mine_a),
        vec!["A shared note", "A private note"],
        "{mine_a}"
    );
    let (_, mine_a2) = w.list(w.a2, "mine", |_| {}).await;
    assert_eq!(
        titles(&mine_a2),
        vec!["A2 shared note", "SECRET A2 private note"],
        "{mine_a2}"
    );
    assert_eq!(
        titles(&mine_a),
        vec!["A shared note", "A private note"],
        "A's atom in A2's ocean is not in A's view"
    );

    // graph_id narrows; an unreadable ocean is a 404 for the list and the detail.
    let (_, only_shared) = w.list(w.a, "team", |p| p.graph_id = Some(w.shared)).await;
    assert_eq!(
        titles(&only_shared),
        vec!["A shared note", "A2 shared note"]
    );
    let (status, _) = w
        .list(w.a, "team", |p| p.graph_id = Some(w.a2_private))
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (_, secret_list) = w.list(w.a2, "mine", |_| {}).await;
    let secret_key = find(&secret_list, "SECRET A2 private note")["key"]
        .as_str()
        .unwrap()
        .to_string();
    let (status, body) = w.detail(w.a, &secret_key, w.a2_private).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    // ... and the key does not open the card through an ocean A can read.
    let (status, _) = w.detail(w.a, &secret_key, w.shared).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, body) = w.detail(w.a2, &secret_key, w.a2_private).await;
    assert_eq!(
        (status, body["title"].clone()),
        (StatusCode::OK, json!("SECRET A2 private note"))
    );

    std::env::remove_var(FLAG);
    w.probe.teardown().await;
}

#[tokio::test]
async fn kinds_grouping_and_meeting_enrichment() {
    let _env = ENV_LOCK.lock().await;
    let Some(w) = world("orgt_activities_kinds").await else {
        eprintln!("skipping kinds_grouping_and_meeting_enrichment: set ORGT_TEST_BASE_URL");
        return;
    };
    std::env::set_var(FLAG, "1");
    let o = &w.probe.owner;

    // A meeting: three transcript chunks, two speakers, a stored summary and duration.
    for (i, who) in [("0", "Tom Becker"), ("1", "Sara Kim"), ("2", "Tom Becker")] {
        w.atom(
            w.shared,
            w.a2,
            "transcript",
            &format!("meet-1:chunk:{i}"),
            at(14, 0),
            Some("Acme pricing review"),
            &[who, "Priya Nair"],
        )
        .await;
    }
    sqlx::query("INSERT INTO meeting_transcripts(graph_id, source_id, raw_inline, duration_ms, summary, summary_status) VALUES ($1, 'meet-1', 'x', $2, $3, 'done')")
        .bind(w.shared)
        .bind(42 * 60_000_i64)
        .bind(json!({"overview": "The team chose usage tiers. Tom owns the rollout.", "decisions": [{"text": "Adopt usage tiers", "ts_ms": 1}], "action_items": []}))
        .execute(o)
        .await
        .unwrap();

    // A colon-bearing coding session: two checkpoints, one card, the session's title.
    sqlx::query("INSERT INTO coding_sessions(graph_id, session_id, title) VALUES ($1, 'team:alpha', 'Rate limiter spike')")
        .bind(w.shared)
        .execute(o)
        .await
        .unwrap();
    w.atom(
        w.shared,
        w.a,
        "coding_session",
        "cc:team:alpha:ckpt-1",
        at(9, 0),
        Some("headline 1"),
        &[],
    )
    .await;
    w.atom(
        w.shared,
        w.a,
        "coding_session",
        "cc:team:alpha:ckpt-2",
        at(9, 40),
        Some("headline 2"),
        &[],
    )
    .await;
    // A Slack thread and an unmapped source type.
    w.atom(
        w.shared,
        w.a,
        "slack",
        "C1/1729000000.000100",
        at(15, 30),
        None,
        &["Aiko"],
    )
    .await;
    w.atom(
        w.shared,
        w.a,
        "gmail",
        "mail-1",
        at(16, 0),
        Some("An email"),
        &[],
    )
    .await;

    let (status, team) = w.list(w.a, "team", |_| {}).await;
    assert_eq!(status, StatusCode::OK, "{team}");
    assert_eq!(
        titles(&team),
        vec!["Rate limiter spike", "Acme pricing review", "Slack thread"],
        "{team}"
    );
    assert!(!team.to_string().contains("An email"), "gmail has no card");

    let session = find(&team, "Rate limiter spike");
    assert_eq!(session["kind"], json!("agent"));
    assert_eq!(session["source_count"], json!(2));
    assert_eq!(
        session["outcome"],
        Value::Null,
        "an agent session with no decision has no outcome"
    );
    assert_eq!(
        (session["start"].clone(), session["end"].clone()),
        (json!("2026-10-29T09:00:00Z"), json!("2026-10-29T09:40:00Z"))
    );

    let meeting = find(&team, "Acme pricing review");
    assert_eq!(meeting["kind"], json!("meeting"));
    assert_eq!(meeting["source_count"], json!(3));
    assert_eq!(
        meeting["people"],
        json!(["Priya Nair", "Sara Kim", "Tom Becker"])
    );
    assert_eq!(meeting["people_count"], json!(3));
    assert_eq!(meeting["outcome"], json!("The team chose usage tiers."));
    assert_eq!(meeting["decision_count"], json!(1));
    assert_eq!(
        meeting["end"],
        json!("2026-10-29T14:42:00Z"),
        "the recorded duration"
    );
    assert_eq!(meeting["owner"]["name"], json!("Tom Becker"));

    let slack = find(&team, "Slack thread");
    assert_eq!(
        (slack["kind"].clone(), slack["kind_label"].clone()),
        (json!("slack"), json!("slack thread"))
    );
    assert_eq!(slack["people"], json!(["Aiko"]));

    // The date strip: 14 days ending on the 29th, today's three cards on the last.
    let strip = team["day_activity"].as_array().unwrap();
    assert_eq!(strip.len(), 14);
    assert_eq!(
        (strip[0]["day"].clone(), strip[13]["day"].clone()),
        (json!("2026-10-16"), json!("2026-10-29"))
    );
    assert_eq!(strip[13]["count"], json!(3));
    assert_eq!(
        strip
            .iter()
            .map(|d| d["count"].as_i64().unwrap())
            .sum::<i64>(),
        3
    );

    // The strip ignores `from`: a one-hour window still reports the whole strip.
    let (_, narrow) = w
        .list(w.a, "team", |p| {
            p.from = Some("2026-10-29T09:00:00Z".into());
            p.to = Some("2026-10-29T10:00:00Z".into());
        })
        .await;
    assert_eq!(titles(&narrow), vec!["Rate limiter spike"]);
    assert_eq!(narrow["day_activity"].as_array().unwrap().len(), 14);

    // The detail of the colon-bearing session finds both checkpoints.
    let (status, detail) = w
        .detail(w.a, session["key"].as_str().unwrap(), w.shared)
        .await;
    assert_eq!(status, StatusCode::OK, "{detail}");
    assert_eq!(detail["source_count"], json!(2));
    let kinds: Vec<_> = detail["sources"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["meta_label"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        kinds,
        vec!["AGENT · 09:00", "AGENT · 09:40"],
        "no extraction yet: the corals' excerpts"
    );

    std::env::remove_var(FLAG);
    w.probe.teardown().await;
}

#[tokio::test]
async fn decisions_sources_and_reefs_come_from_extraction() {
    let _env = ENV_LOCK.lock().await;
    let Some(w) = world("orgt_activities_evidence").await else {
        eprintln!(
            "skipping decisions_sources_and_reefs_come_from_extraction: set ORGT_TEST_BASE_URL"
        );
        return;
    };
    std::env::set_var(FLAG, "1");
    let o = &w.probe.owner;

    w.atom(
        w.shared,
        w.a,
        "note",
        "note-x",
        at(11, 15),
        Some("Pricing note"),
        &[],
    )
    .await;
    let origin = json!({"service_id": "fixture", "transport": "native", "content_kind": "note"});
    let item: Uuid = sqlx::query_scalar("INSERT INTO source_items(graph_id, source_type, provider_namespace, source_id, origin) VALUES ($1,'note','fixture','note-x',$2) RETURNING source_item_id")
        .bind(w.shared)
        .bind(&origin)
        .fetch_one(o)
        .await
        .unwrap();
    let version: Uuid = sqlx::query_scalar(
        "INSERT INTO source_versions(graph_id, source_item_id, source_id, source_type, provider_namespace, origin, raw_inline, extraction_status) \
         VALUES ($1,$2,'note-x','note','fixture',$3,'We chose usage tiers. Limits stay at 40 percent.','done') RETURNING version_id",
    )
    .bind(w.shared)
    .bind(item)
    .bind(&origin)
    .fetch_one(o)
    .await
    .unwrap();
    sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
        .bind(item)
        .bind(version)
        .execute(o)
        .await
        .unwrap();
    let mut atoms = Vec::new();
    for (ordinal, kind, statement, quote, start) in [
        (
            0,
            "fact",
            "Limits stay at 40 percent.",
            "Limits stay at 40 percent.",
            24,
        ),
        (
            1,
            "decision",
            "Adopt usage tiers",
            "We chose usage tiers.",
            0,
        ),
    ] {
        let id: Uuid = sqlx::query_scalar(
            "INSERT INTO semantic_atoms(graph_id, source_version_id, ordinal, kind, statement, extraction_version, model, entities) \
             VALUES ($1,$2,$3,$4,$5,(SELECT extraction_version FROM source_versions WHERE version_id=$2),'fixture',$6) RETURNING semantic_atom_id",
        )
        .bind(w.shared)
        .bind(version)
        .bind(ordinal)
        .bind(kind)
        .bind(statement)
        // The reef's title is shown only when a permitted captured mention
        // names it (`library_node_title`).
        .bind(json!([{"mention": "Pricing model"}]))
        .fetch_one(o)
        .await
        .unwrap();
        sqlx::query("INSERT INTO semantic_atom_passages(semantic_atom_id, source_version_id, graph_id, start_char, end_char, quote) VALUES ($1,$2,$3,$4,$5,$6)")
            .bind(id)
            .bind(version)
            .bind(w.shared)
            .bind(start)
            .bind(start + quote.chars().count() as i32)
            .bind(quote)
            .execute(o)
            .await
            .unwrap();
        atoms.push(id);
    }
    // A reef linked to the decision atom, and a decision-type reef.
    let reef: Uuid = sqlx::query_scalar("INSERT INTO nodes(graph_id, type, title) VALUES ($1,'entity','Pricing model') RETURNING node_id")
        .bind(w.shared)
        .fetch_one(o)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO node_semantic_atoms(graph_id, node_id, semantic_atom_id) VALUES ($1,$2,$3)",
    )
    .bind(w.shared)
    .bind(reef)
    .bind(atoms[1])
    .execute(o)
    .await
    .unwrap();

    let (status, list) = w.list(w.a, "mine", |_| {}).await;
    assert_eq!(status, StatusCode::OK, "{list}");
    let card = find(&list, "Pricing note");
    assert_eq!(card["kind"], json!("note"));
    assert_eq!(card["decision_count"], json!(1));
    assert_eq!(
        card["outcome"],
        json!("Adopt usage tiers"),
        "a note's outcome is its first decision"
    );
    assert_eq!(
        card["topics"],
        json!([{"node_id": reef, "title": "Pricing model"}])
    );

    let (status, detail) = w.detail(w.a, card["key"].as_str().unwrap(), w.shared).await;
    assert_eq!(status, StatusCode::OK, "{detail}");
    assert_eq!(detail["decisions"], json!(["Adopt usage tiers"]));
    assert_eq!(
        detail["reefs"],
        json!([{"node_id": reef, "title": "Pricing model"}])
    );
    let sources = detail["sources"].as_array().unwrap();
    assert_eq!(
        sources.len(),
        3,
        "two passages, then the coral's excerpt: {detail}"
    );
    assert_eq!(
        sources[0]["quote"],
        json!("We chose usage tiers."),
        "decision passages come first"
    );
    assert_eq!(sources[0]["meta_label"], json!("NOTE · 11:15"));
    assert_eq!(sources[0]["source_item_id"], json!(item));
    assert_eq!(sources[0]["atom_id"], json!(atoms[1]));
    assert_eq!(sources[1]["quote"], json!("Limits stay at 40 percent."));
    assert_eq!(sources[2]["quote"], json!("excerpt of note-x"));
    assert_eq!(sources[2]["source_item_id"], Value::Null);

    std::env::remove_var(FLAG);
    w.probe.teardown().await;
}

#[tokio::test]
async fn flag_off_window_limits_and_empty_answers() {
    let _env = ENV_LOCK.lock().await;
    let Some(w) = world("orgt_activities_flag").await else {
        eprintln!("skipping flag_off_window_limits_and_empty_answers: set ORGT_TEST_BASE_URL");
        return;
    };

    std::env::remove_var(FLAG);
    let (status, body) = w.list(w.a, "mine", |_| {}).await;
    assert_eq!(
        (status, body["error"].clone()),
        (StatusCode::NOT_FOUND, json!("not found"))
    );
    let (status, _) = w.detail(w.a, "anything", w.shared).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    std::env::set_var(FLAG, "1");
    // No atoms: 200, no cards, the strip still has its 14 days.
    let (status, empty) = w.list(w.a, "team", |_| {}).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(empty["activities"], json!([]));
    assert_eq!(empty["day_activity"].as_array().unwrap().len(), 14);
    assert_eq!(empty["truncated"], json!(false));
    assert_eq!(empty["window"]["tz"], json!("UTC"));

    for (label, edit) in [
        (
            "63 days",
            Box::new(|p: &mut JournalActivitiesListParams| {
                p.to = Some("2026-12-31T00:00:00Z".into())
            }) as Box<dyn FnOnce(&mut JournalActivitiesListParams)>,
        ),
        (
            "backwards",
            Box::new(|p| p.from = Some("2026-11-01T00:00:00Z".into())),
        ),
        ("no from", Box::new(|p| p.from = None)),
        ("bad who", Box::new(|p| p.who = Some("everyone".into()))),
        ("bad tz", Box::new(|p| p.tz = Some("Mars/Base".into()))),
    ] {
        let (status, body) = w.list(w.a, "mine", edit).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{label}: {body}");
    }
    let (status, _) = w.detail(w.a, "not-a-key", w.shared).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "a malformed key");

    std::env::remove_var(FLAG);
    w.probe.teardown().await;
}

/// The same reads against the TENANT migration tree (what an org's content
/// plane runs), as the database owner in system scope: every statement in the
/// module (grouping, floors, reefs, semantic decisions, meeting summary,
/// passages) has to exist and plan there, not only on the consumer tree.
#[tokio::test]
async fn the_reads_run_on_the_tenant_plane() {
    use coralswarm_backend::services::journal_activities as ja;
    use coralswarm_backend::test_db::{migrated_database, Plane};
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
    use std::str::FromStr;

    let Ok(base) = std::env::var("ORGT_TEST_BASE_URL") else {
        eprintln!("skipping the_reads_run_on_the_tenant_plane: set ORGT_TEST_BASE_URL");
        return;
    };
    let options = PgConnectOptions::from_str(&base).unwrap();
    let db = migrated_database(&options, Plane::Tenant, "orgt_activities_tenant").await;
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options.database(db.name()))
        .await
        .unwrap();

    let (account, user, graph) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    sqlx::query("INSERT INTO graphs(id, account_id, name, kind) VALUES ($1,$2,'Acme','org')")
        .bind(graph)
        .bind(account)
        .execute(&pool)
        .await
        .unwrap();
    for (st, sid, hour) in [
        ("note", "n1", 9),
        ("transcript", "m1:chunk:0", 10),
        ("transcript", "m1:chunk:1", 10),
        ("coding_session", "cc:s1:c1", 11),
    ] {
        sqlx::query("INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, ingested_by, \"timestamp\", participants, excerpt) VALUES ($1,$2,$3,'k',$4,$5,ARRAY['Tom'],'an excerpt')")
            .bind(graph).bind(st).bind(sid).bind(user).bind(at(hour, 0)).execute(&pool).await.unwrap();
    }
    sqlx::query(
        "INSERT INTO coding_sessions(graph_id, session_id, title) VALUES ($1,'s1','Spike')",
    )
    .bind(graph)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO meeting_transcripts(graph_id, source_id, raw_inline, duration_ms, summary, summary_status) VALUES ($1,'m1','x',600000,$2,'done')")
        .bind(graph).bind(json!({"overview": "A short sync. More.", "decisions": [], "action_items": []})).execute(&pool).await.unwrap();

    let graphs = [ja::GraphRef {
        id: graph,
        name: "Acme".into(),
    }];
    let request = ja::ListRequest {
        caller: user,
        who: ja::Who::Team,
        from: at(0, 0),
        to: at(0, 0) + chrono::Duration::days(1),
        tz: chrono_tz::UTC,
        graphs: &graphs,
    };
    let mut tx = pool.begin().await.unwrap();
    let list = ja::list_activities(&mut tx, &request).await.unwrap();
    let got: Vec<_> = list
        .activities
        .iter()
        .map(|a| (a.title.as_str(), a.kind, a.source_count))
        .collect();
    assert_eq!(
        got,
        vec![
            ("Note", ja::Kind::Note, 1),
            ("Meeting", ja::Kind::Meeting, 2),
            ("Spike", ja::Kind::Agent, 1)
        ],
        "{list:?}"
    );
    assert_eq!(list.activities[1].outcome.as_deref(), Some("A short sync."));
    assert_eq!(list.activities[1].end, at(10, 10));
    assert_eq!(list.day_activity.len(), 14);
    for card in &list.activities {
        let detail = ja::activity_detail(
            &mut tx,
            &ja::DetailRequest {
                key: &card.key,
                tz: chrono_tz::UTC,
                graph: &graphs[0],
            },
        )
        .await
        .unwrap();
        assert_eq!(detail.expect("detail").activity.key, card.key);
    }
    tx.rollback().await.unwrap();
}

/// A teammate's source item goes `restricted`: the Library's admission
/// (`library_permitted_atoms`) removes its atoms BEFORE grouping, so the card
/// (title, channel, people, counts) vanishes from Team for another member of
/// the same account, and a stale or crafted key for it opens nothing, not even
/// the corals' raw excerpts. Non-vacuous: the card is visible while permitted.
#[tokio::test]
async fn a_restricted_source_never_reaches_team_or_the_detail_excerpts() {
    let _env = ENV_LOCK.lock().await;
    let Some(w) = world("orgt_activities_restricted").await else {
        eprintln!("skipping a_restricted_source_never_reaches_team_or_the_detail_excerpts: set ORGT_TEST_BASE_URL");
        return;
    };
    std::env::set_var(FLAG, "1");
    let o = &w.probe.owner;

    // A2 ingests a Slack thread into the shared ocean, with its source item.
    w.atom(
        w.shared,
        w.a2,
        "slack",
        "C9/1.1",
        at(10, 0),
        Some("RESTRICTED thread"),
        &["Hidden Person"],
    )
    .await;
    w.atom(
        w.shared,
        w.a2,
        "note",
        "visible-note",
        at(11, 0),
        Some("Visible note"),
        &[],
    )
    .await;
    let item: Uuid = sqlx::query_scalar(
        "INSERT INTO source_items(graph_id, source_type, provider_namespace, source_id, origin, access_state) \
         VALUES ($1,'slack','fixture','C9/1.1',$2,'permitted') RETURNING source_item_id",
    )
    .bind(w.shared)
    .bind(json!({"service_id": "fixture", "transport": "native", "content_kind": "message"}))
    .fetch_one(o)
    .await
    .unwrap();

    // While permitted, A (a different member) sees it in Team and can open it.
    let (_, before) = w.list(w.a, "team", |_| {}).await;
    assert_eq!(
        titles(&before),
        vec!["RESTRICTED thread", "Visible note"],
        "{before}"
    );
    let key = find(&before, "RESTRICTED thread")["key"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(key, ja_key("slack", "C9/1.1"));
    let (status, detail) = w.detail(w.a, &key, w.shared).await;
    assert_eq!(status, StatusCode::OK, "{detail}");
    assert_eq!(detail["sources"][0]["quote"], json!("excerpt of C9/1.1"));

    sqlx::query("UPDATE source_items SET access_state='restricted' WHERE source_item_id=$1")
        .bind(item)
        .execute(o)
        .await
        .unwrap();

    for who in ["team", "mine"] {
        for reader in [w.a, w.a2] {
            let (status, after) = w.list(reader, who, |_| {}).await;
            assert_eq!(status, StatusCode::OK);
            let text = after.to_string();
            assert!(
                !text.contains("RESTRICTED")
                    && !text.contains("Hidden Person")
                    && !text.contains("C9/1.1"),
                "{who}: a restricted source leaked: {text}"
            );
            assert!(
                titles(&after).contains(&"Visible note".to_string())
                    || who == "mine" && reader == w.a,
                "{after}"
            );
        }
    }
    // The strip no longer counts it either (only the visible note is left).
    let (_, team) = w.list(w.a, "team", |_| {}).await;
    assert_eq!(team["day_activity"][13]["count"], json!(1));

    // A stale or crafted key: 404, no excerpt.
    for reader in [w.a, w.a2] {
        let (status, body) = w.detail(reader, &key, w.shared).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
        assert!(!body.to_string().contains("excerpt of"), "{body}");
    }

    std::env::remove_var(FLAG);
    w.probe.teardown().await;
}

fn ja_key(source_type: &str, grouping_key: &str) -> String {
    coralswarm_backend::services::journal_activities::make_key(source_type, grouping_key)
}
