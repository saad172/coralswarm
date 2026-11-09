//! Live-Postgres proof of chat find (`services::find_results`): the
//! `source_results` tiles `attach_to_answer` adds to a scoped answer and
//! `GET /v1/find/titles`, driven through the REAL handler and service over the
//! `coralswarm_app` login (RLS applies as in production) with a hand-built
//! `UserContext`, because "two members of one account" is a shape a plain
//! personal Clerk token cannot produce.
//!
//! Fixture: one account with two members (A the owner, A2), one shared ocean
//! and one private (`member`) ocean per member.
//!
//!   1. Tiles: citations group per SOURCE (three passages of one meeting are
//!      one tile), rank by score then order, cap at three, mark rank 1
//!      `best`, and enrich (kind, title, date, recorded duration, people,
//!      ocean, outcome from the summary, decisions, quotes). A legacy chunk
//!      resolves to its source by dropping `:chunk:N`. An unmapped source
//!      type, and a citation of another member's private ocean, make no tile
//!      and the secret appears nowhere.
//!   2. The field is absent with the flag off, and for a question that is not
//!      a find question.
//!   3. Title search: case-insensitive, `%`/`_` literal, exact and prefix
//!      first, limit, validation, flag 404, and isolation (A2's private
//!      ocean never appears for A, and vice versa; non-vacuous).
//!
//! Env-gated on `ORGT_TEST_BASE_URL`:
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test find_results_pg

mod support;

use std::sync::Arc;

use axum::extract::Query;
use axum::http::StatusCode;
use chrono::{DateTime, TimeZone, Utc};
use serde_json::{json, Value};
use sqlx::PgPool;
use tokio::sync::Mutex;
use uuid::Uuid;

use coralswarm_backend::services::find_results as fr;
use coralswarm_backend::store::PostgresStore;
use coralswarm_backend::tenancy::extractor::Tenant;
use coralswarm_backend::{find_titles, FindTitlesParams, UserContext};
use support::rls_probe::RlsProbe;

/// Serializes the shared-process `CHAT_FIND_RESULTS_ENABLED` mutations.
static ENV_LOCK: Mutex<()> = Mutex::const_new(());

fn at(day: u32, h: u32, m: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, day, h, m, 0).unwrap()
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

async fn user(owner: &PgPool, tag: &str, name: &str) -> Uuid {
    sqlx::query_scalar("INSERT INTO users(email, name) VALUES ($1, $2) RETURNING id")
        .bind(format!("{tag}_{}@find.example", Uuid::new_v4().simple()))
        .bind(name)
        .fetch_one(owner)
        .await
        .expect("insert user")
}

async fn world(db: &str) -> Option<World> {
    let probe = RlsProbe::setup(db).await?;
    let o = &probe.owner;
    let a = user(o, "a", "Priya Nair").await;
    let a2 = user(o, "a2", "Tom Becker").await;
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
    let shared: Uuid =
        sqlx::query_scalar("INSERT INTO graphs (account_id, name) VALUES ($1, 'GTM') RETURNING id")
            .bind(account)
            .fetch_one(o)
            .await
            .unwrap();
    let mut private = Vec::new();
    for (u, name) in [(a, "Private A"), (a2, "Private A2")] {
        private.push(
            sqlx::query_scalar::<_, Uuid>(
                "INSERT INTO graphs (account_id, name, kind, owner_user_id) VALUES ($1, $2, 'member', $3) RETURNING id",
            )
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

    fn ctx(&self, user_id: Uuid) -> UserContext {
        UserContext {
            user_id,
            account_id: self.account,
            role: "member".into(),
            device_id: "test".into(),
        }
    }

    /// A source item with one current version: `(source_item_id, version_id)`.
    async fn source(
        &self,
        graph: Uuid,
        source_type: &str,
        source_id: &str,
        title: Option<&str>,
        when: DateTime<Utc>,
    ) -> (Uuid, Uuid) {
        let o = &self.probe.owner;
        let item: Uuid = sqlx::query_scalar(
            "INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id) VALUES($1,$2,'fixture:find',$3) RETURNING source_item_id",
        )
        .bind(graph)
        .bind(source_type)
        .bind(source_id)
        .fetch_one(o)
        .await
        .expect("source item");
        let metadata = match title {
            Some(t) => json!({ "title": t }),
            None => json!({}),
        };
        let version: Uuid = sqlx::query_scalar(
            "INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,raw_inline,content_sha256,representation,source_metadata,occurred_at,extraction_status) \
             VALUES($1,$2,$3,$4,'fixture:find',$5,encode(sha256(convert_to($5,'UTF8')),'hex'),'normalized_text',$6,$7,'done') RETURNING version_id",
        )
        .bind(graph)
        .bind(item)
        .bind(source_id)
        .bind(source_type)
        .bind(format!("body of {source_id}"))
        .bind(metadata)
        .bind(when)
        .fetch_one(o)
        .await
        .expect("source version");
        sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
            .bind(item)
            .bind(version)
            .execute(o)
            .await
            .expect("current version");
        (item, version)
    }

    async fn atom(
        &self,
        graph: Uuid,
        by: Uuid,
        source_type: &str,
        source_id: &str,
        when: DateTime<Utc>,
        participants: &[&str],
    ) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, ingested_by, \"timestamp\", participants, excerpt) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING atom_id",
        )
        .bind(graph)
        .bind(source_type)
        .bind(source_id)
        .bind(format!("blob-{source_id}"))
        .bind(by)
        .bind(when)
        .bind(participants.iter().map(|p| p.to_string()).collect::<Vec<_>>())
        .bind(format!("excerpt of {source_id}"))
        .fetch_one(&self.probe.owner)
        .await
        .expect("insert atom")
    }

    async fn decision(&self, graph: Uuid, version: Uuid, statement: &str) {
        sqlx::query(
            "INSERT INTO semantic_atoms(semantic_atom_id,graph_id,source_version_id,ordinal,kind,statement,extraction_version,model) \
             VALUES(gen_random_uuid(),$1,$2,0,'decision',$3,'source-atoms-v1','fixture')",
        )
        .bind(graph)
        .bind(version)
        .bind(statement)
        .execute(&self.probe.owner)
        .await
        .expect("decision atom");
    }

    async fn titles(
        &self,
        user_id: Uuid,
        q: Option<&str>,
        limit: Option<i64>,
    ) -> (StatusCode, Value) {
        let resp = find_titles(
            self.tenant(),
            axum::Extension(self.ctx(user_id)),
            Query(FindTitlesParams {
                q: q.map(str::to_string),
                limit,
            }),
        )
        .await;
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    async fn tiles(&self, user_id: Uuid, question: &str, payload: &mut Value) {
        fr::attach_to_answer(&self.tenant(), &self.ctx(user_id), question, payload).await;
    }
}

fn cite(graph: Uuid, item: Uuid, quote: &str, score: Option<f64>) -> Value {
    let mut c = json!({
        "kind": "source_passage", "graph_id": graph, "source_item_id": item,
        "passage_id": Uuid::new_v4(), "quote": quote, "occurred_at": "2026-10-15T15:18:00Z",
    });
    if let Some(s) = score {
        c["score"] = json!(s);
    }
    c
}

fn titles_of(body: &Value) -> Vec<String> {
    body["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["title"].as_str().unwrap().to_string())
        .collect()
}

const FIND: &str = "Find the Acme pricing meeting from two weeks ago";

#[tokio::test]
async fn tiles_group_rank_enrich_and_never_leak() {
    let _env = ENV_LOCK.lock().await;
    let Some(w) = world("orgt_find_tiles").await else {
        eprintln!("skipping tiles_group_rank_enrich_and_never_leak: set ORGT_TEST_BASE_URL");
        return;
    };
    std::env::set_var(fr::FLAG, "1");
    let o = &w.probe.owner;

    // A meeting with a stored summary and duration, two speakers, one decision.
    let (meeting, meeting_v) = w
        .source(
            w.shared,
            "transcript",
            "meet-1",
            Some("Acme pricing review"),
            at(15, 15, 0),
        )
        .await;
    // The version's own participants join the atoms' names.
    sqlx::query("UPDATE source_versions SET participants=ARRAY['Dana Kim'] WHERE version_id=$1")
        .bind(meeting_v)
        .execute(o)
        .await
        .expect("version participants");
    for (i, who) in [("0", "Tom Becker"), ("1", "Sara Kim")] {
        w.atom(
            w.shared,
            w.a2,
            "transcript",
            &format!("meet-1:chunk:{i}"),
            at(15, 15, 0),
            &[who, "Priya Nair"],
        )
        .await;
    }
    // Not people of the meeting: a retracted chunk's participant, and a
    // different-type source that shares the source_id string.
    let retracted = w
        .atom(
            w.shared,
            w.a2,
            "transcript",
            "meet-1:chunk:8",
            at(15, 15, 0),
            &["Retracted Person"],
        )
        .await;
    sqlx::query("UPDATE atoms SET evidence_retracted=true WHERE atom_id=$1")
        .bind(retracted)
        .execute(o)
        .await
        .expect("retract");
    w.atom(
        w.shared,
        w.a2,
        "note",
        "meet-1:chunk:9",
        at(15, 15, 0),
        &["Other Type"],
    )
    .await;
    // A Slack thread (cited only through a legacy chunk), a note with an
    // extracted decision, a fourth source that must fall off the cap, an
    // unmapped type, and a secret in A2's private ocean.
    let (slack, _) = w
        .source(
            w.shared,
            "slack",
            "C1/1729000000.000100",
            Some("#gtm Acme pricing prep"),
            at(14, 16, 40),
        )
        .await;
    let legacy_atom = w
        .atom(
            w.shared,
            w.a,
            "slack",
            "C1/1729000000.000100:chunk:0",
            at(14, 16, 40),
            &["Priya Nair"],
        )
        .await;
    let (note, note_v) = w
        .source(
            w.shared,
            "note",
            "note-1",
            Some("Acme follow-up note"),
            at(16, 9, 0),
        )
        .await;
    w.decision(w.shared, note_v, "Offer Acme a soft usage cap")
        .await;
    let (fourth, _) = w
        .source(w.shared, "note", "note-2", Some("Acme extra"), at(17, 9, 0))
        .await;
    let (mail, _) = w
        .source(
            w.shared,
            "gmail",
            "mail-1",
            Some("Acme email"),
            at(17, 10, 0),
        )
        .await;
    let (secret, _) = w
        .source(
            w.a2_private,
            "note",
            "secret-1",
            Some("SECRET Acme pricing"),
            at(17, 11, 0),
        )
        .await;

    // Publish the summary after fixture source-pointer mutations. L4's durable
    // reduction fences correctly clear earlier compiled prose on those writes.
    sqlx::query("INSERT INTO meeting_transcripts(graph_id, source_id, source_item_id, current_version_id, raw_inline, duration_ms, summary, summary_status) VALUES ($1,'meet-1',$2,$3,'x',$4,$5,'done')")
        .bind(w.shared)
        .bind(meeting)
        .bind(meeting_v)
        .bind(42 * 60_000_i64)
        .bind(json!({"overview": "Acme pushed back on a hard usage cap. Three tiers proposed.", "decisions": [{"text": "Propose three tiers to Acme"}], "action_items": []}))
        .execute(o)
        .await
        .expect("meeting transcript");
    let mut payload = json!({
        "retained_citations": [
            cite(w.shared, note, "A hard cap is a non-starter - soft we can live with.", None),
            cite(w.shared, meeting, "Can we see this as three tiers?", Some(0.9)),
            cite(w.shared, meeting, "A hard cap will not get through procurement.", Some(0.4)),
            cite(w.shared, meeting, "Can we see this as three tiers?", None),
            cite(w.shared, fourth, "extra", None),
            cite(w.shared, mail, "mail", Some(5.0)),
            cite(w.a2_private, secret, "SECRET quote", Some(9.0)),
        ],
        "legacy_evidence": [{"graph_id": w.shared, "atom_id": legacy_atom}],
    });
    w.tiles(w.a, FIND, &mut payload).await;

    let tiles = payload["source_results"]
        .as_array()
        .unwrap_or_else(|| panic!("no source_results in {payload}"))
        .clone();
    assert_eq!(tiles.len(), 3, "{payload}");
    let leaked = payload["source_results"].to_string();
    assert!(
        !leaked.contains("SECRET") && !leaked.contains(&w.a2_private.to_string()),
        "private ocean leaked: {leaked}"
    );
    assert!(
        !leaked.contains(&mail.to_string()),
        "an unmapped type has no tile"
    );

    // Rank: the meeting (best citation score 0.9), then first appearance: the
    // note (order 0), then the Slack thread (legacy chunk, after the rest) -
    // and the fourth source falls off the cap.
    let ids: Vec<String> = tiles
        .iter()
        .map(|t| t["source_item_id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        ids,
        vec![meeting.to_string(), note.to_string(), fourth.to_string()],
        "{leaked}"
    );
    assert!(
        !ids.contains(&slack.to_string()),
        "the legacy chunk ranks after the fourth source's passage"
    );
    assert_eq!(
        tiles
            .iter()
            .map(|t| t["best"].as_bool().unwrap())
            .collect::<Vec<_>>(),
        vec![true, false, false]
    );

    let m = &tiles[0];
    assert_eq!(
        (m["kind"].clone(), m["kind_label"].clone()),
        (json!("meeting"), json!("meeting"))
    );
    assert_eq!(m["title"], json!("Acme pricing review"));
    assert_eq!(m["outcome"], json!("Acme pushed back on a hard usage cap."));
    assert_eq!(m["duration_ms"], json!(42 * 60_000));
    assert_eq!(m["occurred_at"], json!("2026-10-15T15:00:00Z"));
    assert_eq!(
        m["people"],
        json!(["Dana Kim", "Priya Nair", "Sara Kim", "Tom Becker"])
    );
    assert_eq!(m["graph"], json!({"id": w.shared, "name": "GTM"}));
    assert_eq!(m["decisions"], json!(["Propose three tiers to Acme"]));
    assert_eq!(
        m["quotes"].as_array().unwrap().len(),
        2,
        "three passages, one repeated: two distinct quotes on one tile"
    );

    let n = &tiles[1];
    assert_eq!(n["kind"], json!("note"));
    assert_eq!(
        n["outcome"],
        json!("Offer Acme a soft usage cap"),
        "no summary: the first decision"
    );
    assert_eq!(n["duration_ms"], Value::Null);
    let f = &tiles[2];
    assert_eq!(f["outcome"], Value::Null, "nothing stored to say: null");

    // The Slack thread becomes a tile once the cap leaves room for it.
    let mut only_legacy =
        json!({"legacy_evidence": [{"graph_id": w.shared, "atom_id": legacy_atom}]});
    w.tiles(
        w.a,
        "Where did we discuss Acme pricing prep?",
        &mut only_legacy,
    )
    .await;
    let t = &only_legacy["source_results"][0];
    assert_eq!(
        (
            t["source_item_id"].clone(),
            t["kind"].clone(),
            t["kind_label"].clone()
        ),
        (json!(slack), json!("slack"), json!("slack thread"))
    );
    assert_eq!(t["best"], json!(true));

    // The secret is A2's own tile when A2 asks (non-vacuous).
    let mut theirs =
        json!({"retained_citations": [cite(w.a2_private, secret, "SECRET quote", None)]});
    w.tiles(w.a2, FIND, &mut theirs).await;
    assert_eq!(
        theirs["source_results"][0]["title"],
        json!("SECRET Acme pricing")
    );

    // Not a find question: absent. A find question with nothing readable: [].
    let mut other = json!({"retained_citations": [cite(w.shared, meeting, "q", None)]});
    w.tiles(w.a, "Why did billing move to usage-based?", &mut other)
        .await;
    assert!(other.get("source_results").is_none(), "{other}");
    let mut empty =
        json!({"retained_citations": [cite(w.a2_private, secret, "SECRET quote", None)]});
    w.tiles(w.a, FIND, &mut empty).await;
    assert_eq!(empty["source_results"], json!([]));

    // Flag off: absent for the very same payload.
    std::env::remove_var(fr::FLAG);
    let mut off = json!({"retained_citations": [cite(w.shared, meeting, "q", None)]});
    w.tiles(w.a, FIND, &mut off).await;
    assert!(
        off.get("source_results").is_none(),
        "flag off must emit no field: {off}"
    );

    w.probe.teardown().await;
}

#[tokio::test]
async fn title_search_is_title_only_ranked_bounded_and_isolated() {
    let _env = ENV_LOCK.lock().await;
    let Some(w) = world("orgt_find_titles").await else {
        eprintln!("skipping title_search_is_title_only_ranked_bounded_and_isolated: set ORGT_TEST_BASE_URL");
        return;
    };
    std::env::set_var(fr::FLAG, "1");

    w.source(
        w.shared,
        "transcript",
        "m1",
        Some("Acme pricing review"),
        at(15, 15, 0),
    )
    .await;
    w.source(
        w.shared,
        "transcript",
        "m2",
        Some("Weekly sync about Acme"),
        at(20, 10, 0),
    )
    .await;
    w.source(w.shared, "note", "n1", Some("acme"), at(10, 9, 0))
        .await;
    w.source(
        w.shared,
        "slack",
        "s1",
        Some("#gtm Acme prep"),
        at(14, 9, 0),
    )
    .await;
    w.source(w.shared, "gmail", "g1", Some("Acme invoice"), at(14, 9, 0))
        .await;
    w.source(w.shared, "note", "n2", Some("100% done_ok"), at(14, 9, 0))
        .await;
    w.source(w.shared, "note", "n3", Some("Roadmap"), at(14, 9, 0))
        .await;
    w.source(
        w.a_private,
        "coding_session",
        "a1",
        Some("Acme rate limiter spike"),
        at(21, 9, 0),
    )
    .await;
    w.source(
        w.a2_private,
        "note",
        "p1",
        Some("SECRET Acme pricing"),
        at(22, 9, 0),
    )
    .await;
    // Body text is not searched: only titles.
    w.atom(w.shared, w.a, "note", "n3", at(14, 9, 0), &["Acme"])
        .await;

    // A: shared + own private; exact title first, then prefix, then newest.
    let (status, body) = w.titles(w.a, Some("ACME"), None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        titles_of(&body),
        vec![
            "acme",
            "Acme rate limiter spike",
            "Acme pricing review",
            "Weekly sync about Acme",
            "#gtm Acme prep"
        ],
        "{body}"
    );
    assert!(
        !body.to_string().contains("SECRET") && !body.to_string().contains("Acme invoice"),
        "{body}"
    );
    assert_eq!(body["results"][0]["best"], json!(false));
    assert_eq!(body["results"][0]["kind"], json!("note"));
    assert_eq!(body["results"][1]["kind_label"], json!("agent session"));
    assert_eq!(
        body["results"][1]["graph"],
        json!({"id": w.a_private, "name": "Private A"})
    );
    assert_eq!(body["results"][3]["outcome"], Value::Null);
    assert_eq!(body["results"][3]["quotes"], json!([]));

    // A2: symmetric (non-vacuous: sees its own secret, never A's private).
    let (_, body2) = w.titles(w.a2, Some("acme"), None).await;
    assert!(
        titles_of(&body2).contains(&"SECRET Acme pricing".to_string()),
        "{body2}"
    );
    assert!(!body2.to_string().contains("rate limiter"), "{body2}");

    // Limit, wildcards as literals, body text unsearched, blank and long q.
    let (_, limited) = w.titles(w.a, Some("acme"), Some(2)).await;
    assert_eq!(titles_of(&limited).len(), 2);
    let (_, pct) = w.titles(w.a, Some("%"), None).await;
    assert_eq!(titles_of(&pct), vec!["100% done_ok"], "{pct}");
    let (_, under) = w.titles(w.a, Some("d_n"), None).await;
    assert!(titles_of(&under).is_empty(), "_ is not a wildcard: {under}");
    let (_, bodytext) = w.titles(w.a, Some("excerpt of"), None).await;
    assert!(titles_of(&bodytext).is_empty());
    assert_eq!(w.titles(w.a, None, None).await.0, StatusCode::BAD_REQUEST);
    assert_eq!(
        w.titles(w.a, Some("   "), None).await.0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        w.titles(w.a, Some(&"x".repeat(201)), None).await.0,
        StatusCode::BAD_REQUEST
    );

    // Flag off: 404.
    std::env::remove_var(fr::FLAG);
    assert_eq!(
        w.titles(w.a, Some("acme"), None).await.0,
        StatusCode::NOT_FOUND
    );

    w.probe.teardown().await;
}

#[tokio::test]
async fn title_search_is_narrowed_to_the_active_account() {
    let _env = ENV_LOCK.lock().await;
    let Some(w) = world("orgt_find_account").await else {
        eprintln!(
            "skipping title_search_is_narrowed_to_the_active_account: set ORGT_TEST_BASE_URL"
        );
        return;
    };
    std::env::set_var(fr::FLAG, "1");
    let o = &w.probe.owner;
    // A is also a member of a second account with its own ocean and source.
    let owner_b = user(o, "b", "Other Owner").await;
    let account_b: Uuid =
        sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
            .bind(owner_b)
            .fetch_one(o)
            .await
            .unwrap();
    for (u, role) in [(owner_b, "owner"), (w.a, "member")] {
        sqlx::query("INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, $3)")
            .bind(account_b)
            .bind(u)
            .bind(role)
            .execute(o)
            .await
            .unwrap();
    }
    let graph_b: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name) VALUES ($1, 'Other account') RETURNING id",
    )
    .bind(account_b)
    .fetch_one(o)
    .await
    .unwrap();
    w.source(
        w.shared,
        "note",
        "in-a",
        Some("Acme in account A"),
        at(15, 9, 0),
    )
    .await;
    w.source(
        graph_b,
        "note",
        "in-b",
        Some("Acme in account B"),
        at(15, 9, 0),
    )
    .await;

    let (_, in_a) = w.titles(w.a, Some("acme"), None).await;
    assert_eq!(titles_of(&in_a), vec!["Acme in account A"], "{in_a}");
    let mut as_b = w.ctx(w.a);
    as_b.account_id = account_b;
    let resp = find_titles(
        w.tenant(),
        axum::Extension(as_b),
        Query(FindTitlesParams {
            q: Some("acme".into()),
            limit: None,
        }),
    )
    .await;
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let in_b: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        titles_of(&in_b),
        vec!["Acme in account B"],
        "non-vacuous: {in_b}"
    );

    std::env::remove_var(fr::FLAG);
    w.probe.teardown().await;
}
