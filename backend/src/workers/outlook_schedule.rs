//! Outlook compose capture v2 follower. Called from the tenant Teams worker tick
//! (`teams_meeting_sync::tick`) for each member that has due intents.
//!
//! One pass over one intent:
//! * `pending_save`: find the saved event by its nonce, then start tracking it;
//! * `active`: re-read the event by its immutable id and follow what changed (moved
//!   times, a new Teams link, a series' next occurrence) or stop (cancelled / deleted /
//!   series ended / finished).
//!
//! Every state change goes through `services::outlook_schedule`, which re-checks under a
//! row lock that the intent is still open, so a Stop that lands mid-pass always wins.
use chrono::{DateTime, Duration, Utc};
use reqwest::Client;
use sqlx::PgPool;
use uuid::Uuid;

use crate::connectors::outlook_schedule::{self as graph_events, Error as GraphError, EventView, Kind, SeriesNext};
use crate::connectors::teams_targeted_transcripts::{self as teams, Meeting, Request};
use crate::services::outlook_schedule::{self as svc, Close, Commit, Intent, Plan};

const PENDING_RETRY: Duration = Duration::seconds(30);
const TRANSIENT_RETRY: Duration = Duration::minutes(2);
/// A one-off meeting stays tracked until its transcript window (end + 24h) is over.
const COMPLETE_AFTER: Duration = Duration::hours(24);

/// Everything one pass needs besides the intent itself.
pub(crate) struct Env<'a> {
    pub http: &'a Client,
    pub graph: &'a str,
    pub pool: &'a PgPool,
    pub token: &'a str,
    pub now: DateTime<Utc>,
}


/// What to do after reading the event, before touching the capture.
enum Verdict {
    Track(EventView),
    Done,
}

pub(crate) async fn process(env: &Env<'_>, intent: &Intent) -> Result<(), String> {
    let result = match intent.status.as_str() {
        "pending_save" => pending(env, intent).await,
        "active" => active(env, intent).await,
        _ => Ok(()),
    };
    result.map_err(|e| e.to_string())
}

async fn retry(env: &Env<'_>, intent: &Intent, after: Duration, code: Option<&str>) -> sqlx::Result<()> {
    // An advisory code set while pending (teams_link_missing) survives; transient errors
    // never overwrite what the user already sees.
    let code = code.or(intent.error_code.as_deref());
    svc::set_check(env.pool, intent.account_id, intent.user_id, intent.id, env.now + after, code).await
}

async fn close(env: &Env<'_>, intent: &Intent, status: &'static str, code: &str, revoke: bool) -> sqlx::Result<()> {
    svc::close(
        env.pool,
        intent.account_id,
        intent.user_id,
        intent.id,
        Close { status, code: Some(code), revoke },
    )
    .await
    .map(|_| ())
}

async fn pending(env: &Env<'_>, intent: &Intent) -> sqlx::Result<()> {
    if intent.expires_at <= env.now {
        return svc::close(
            env.pool,
            intent.account_id,
            intent.user_id,
            intent.id,
            Close { status: "expired", code: None, revoke: false },
        )
        .await
        .map(|_| ());
    }
    let event = match graph_events::discover(env.http, env.token, env.graph, intent.nonce).await {
        Ok(Some(ev)) => ev,
        // Not saved (or not visible yet): keep waiting until the intent expires.
        Ok(None) => return retry(env, intent, PENDING_RETRY, None).await,
        Err(GraphError::Ambiguous) => return close(env, intent, "error", "ambiguous_nonce", false).await,
        Err(GraphError::Unsupported) => return close(env, intent, "error", "unsupported_event", false).await,
        Err(_) => return retry(env, intent, PENDING_RETRY, None).await,
    };
    match vet(env, intent, event, true).await? {
        Verdict::Track(ev) => track(env, intent, ev).await,
        Verdict::Done => Ok(()),
    }
}

async fn active(env: &Env<'_>, intent: &Intent) -> sqlx::Result<()> {
    let Some(event_id) = intent.event_id.as_deref() else {
        return close(env, intent, "error", "unsupported_event", false).await;
    };
    let event = match graph_events::get_event(env.http, env.token, env.graph, event_id).await {
        Ok(ev) => ev,
        // A single 404 is not proof: confirm before dropping a real meeting.
        Err(GraphError::Gone) => return gone(env, intent).await,
        Err(GraphError::Unsupported) => return close(env, intent, "stopped", "unsupported_event", true).await,
        // Includes Ambiguous (a mismatched object): change nothing, look again.
        Err(_) => return retry(env, intent, TRANSIENT_RETRY, None).await,
    };
    if intent.gone_checks > 0 {
        svc::clear_gone(env.pool, intent.account_id, intent.user_id, intent.id).await?;
    }
    match vet(env, intent, event, false).await? {
        Verdict::Track(ev) => track(env, intent, ev).await,
        Verdict::Done => Ok(()),
    }
}

/// The tracked event answered 404. Re-run nonce discovery (it may have been re-created
/// under a new id) and otherwise only stop once the 404 has persisted across several checks
/// spanning a few minutes. Covers a series master the same way.
async fn gone(env: &Env<'_>, intent: &Intent) -> sqlx::Result<()> {
    match graph_events::discover(env.http, env.token, env.graph, intent.nonce).await {
        // Re-created under a new id: adopt it (commit_tracking resets the 404 counter).
        Ok(Some(ev)) if intent.event_id.as_deref() != Some(ev.id.as_str()) => {
            tracing::info!("outlook schedule: tracked event re-found by nonce under a new id");
            return match vet(env, intent, ev, false).await? {
                Verdict::Track(ev) => track(env, intent, ev).await,
                Verdict::Done => Ok(()),
            };
        }
        // Definitive: nothing carries our nonce, or only the id that just 404'd (an
        // inconsistent read, not a re-creation). The 404 counts toward confirmation.
        Ok(None) | Ok(Some(_)) => {}
        // Discovery did not complete (transient error, paging cap, our nonce on several
        // events, an unsupported replacement): a replacement may exist that we could not
        // see. Look again soon WITHOUT advancing the 404 count.
        Err(_) => return retry(env, intent, TRANSIENT_RETRY, None).await,
    }
    match svc::note_gone(env.pool, intent.account_id, intent.user_id, intent.id).await? {
        Some(true) => close(env, intent, "stopped", "event_deleted", true).await,
        // Unconfirmed (retry already scheduled) or no longer active.
        _ => Ok(()),
    }
}

/// Organizer / cancelled checks common to first resolve and every later poll.
async fn vet(env: &Env<'_>, intent: &Intent, ev: EventView, first: bool) -> sqlx::Result<Verdict> {
    if !ev.is_organizer {
        // Only the organizer's calendar is consent for the meeting. Lost organizer later
        // (ownership transfer) stops capture instead of reporting a failure.
        let (status, revoke) = if first { ("error", false) } else { ("stopped", true) };
        close(env, intent, status, "not_organizer", revoke).await?;
        return Ok(Verdict::Done);
    }
    if ev.is_cancelled {
        close(env, intent, "stopped", "event_cancelled", true).await?;
        return Ok(Verdict::Done);
    }
    Ok(Verdict::Track(ev))
}

/// Decide the occurrence, resolve the Teams meeting, commit, then try auto-transcription.
async fn track(env: &Env<'_>, intent: &Intent, ev: EventView) -> sqlx::Result<()> {
    let first = intent.status == "pending_save";
    let Some(join_url) = ev.join_url.clone() else {
        // No Teams meeting (yet / any more). Keep whatever is captured, keep looking.
        return retry(env, intent, if first { PENDING_RETRY } else { Duration::minutes(5) }, Some("teams_link_missing")).await;
    };
    let recurring = ev.kind == Kind::SeriesMaster;

    // The occurrence window.
    let (start, end, transcript_end) = if recurring {
        match graph_events::next_occurrence(env.http, env.token, env.graph, &ev, env.now).await {
            Ok(SeriesNext::Found(o)) => (o.start, o.end, Some(o.transcript_end)),
            Ok(SeriesNext::Ended) => {
                return if first {
                    close(env, intent, "error", "already_ended", false).await
                } else {
                    close(env, intent, "stopped", "series_ended", true).await
                };
            }
            // Open-ended series with nothing in the look-ahead: check again later.
            Ok(SeriesNext::NoneYet) => return retry(env, intent, Duration::hours(6), None).await,
            Err(_) => return retry(env, intent, TRANSIENT_RETRY, None).await,
        }
    } else {
        if ev.end + COMPLETE_AFTER <= env.now {
            return if first {
                close(env, intent, "error", "already_ended", false).await
            } else {
                // Natural end: the capture's own window handles the last fetches.
                close(env, intent, "stopped", "completed", false).await
            };
        }
        (ev.start, ev.end, None)
    };

    // Identify the Teams meeting. Only when the link is new to us.
    let meeting_id = match (&intent.join_url, &intent.meeting_id) {
        (Some(old), Some(id)) if *old == join_url => id.clone(),
        _ => match resolve(env, &join_url).await {
            Ok(m) => m.id,
            Err(Resolve::Unsupported) => return close(env, intent, "error", "unsupported_join_url", false).await,
            Err(Resolve::Retry) => return retry(env, intent, if first { PENDING_RETRY } else { TRANSIENT_RETRY }, None).await,
        },
    };

    let plan = Plan {
        event_id: ev.id.clone(),
        recurring,
        join_url: join_url.clone(),
        meeting_id,
        start,
        end,
        transcript_end,
        next_check_at: svc::recheck_after(Some(start), env.now),
    };
    let commit = svc::commit_tracking(env.pool, intent.account_id, intent.user_id, intent.id, &plan).await?;
    let Commit::Tracking { meeting_changed, .. } = commit else {
        if commit == Commit::Conflict {
            return close(env, intent, "error", "capture_conflict", false).await;
        }
        // UserStopped / NotOpen: the service already wrote the right state.
        return Ok(());
    };

    // Transcription is always manual (#1119: CoralSwarm never changes a meeting's
    // recordAutomatically). Recorded once per tracked meeting so the pane can show the
    // recording prompt; a series reports `recurring`, a one-off carries no reason.
    if meeting_changed || intent.auto_transcription.is_none() {
        let reason = recurring.then_some("recurring");
        svc::record_transcription(env.pool, intent.account_id, intent.user_id, intent.id, "manual", reason).await?;
    }
    Ok(())
}

enum Resolve {
    Unsupported,
    Retry,
}

async fn resolve(env: &Env<'_>, join_url: &str) -> Result<Meeting, Resolve> {
    let target = Request {
        meeting_id: None,
        join_url: Some(join_url.to_owned()),
        meeting_ticket: None,
        chat_id: None,
    }
    .target()
    .map_err(|_| Resolve::Unsupported)?;
    teams::resolve_at(env.http, env.token, target, env.graph)
        .await
        .map_err(|e| match e {
            teams::Error::InvalidTarget => Resolve::Unsupported,
            // A freshly saved meeting can take a moment to appear under onlineMeetings (404),
            // and every other failure is transient too: never invent an identity.
            _ => Resolve::Retry,
        })
}

/// One member's due intents. `token` is the member's own delegated Microsoft token.
pub(crate) async fn run_member(env: &Env<'_>, intents: Vec<Intent>) -> usize {
    let mut handled = 0;
    for intent in intents {
        handled += 1;
        if let Err(error) = process(env, &intent).await {
            // The claim lease already pushed next_check_at out; just log (no identifiers).
            tracing::warn!(%error, "outlook schedule pass failed");
        }
    }
    handled
}

/// Convenience for the tick: the identity-checked claim.
pub(crate) async fn claim(
    pool: &PgPool,
    account: Uuid,
    member: Uuid,
    tenant: &str,
    aad: &str,
) -> sqlx::Result<Vec<Intent>> {
    svc::claim_due(pool, account, member, tenant, aad).await
}

/// The member's own delegated Microsoft token (same sequence as the capture poller).
async fn member_token(
    state: &std::sync::Arc<crate::AppState>,
    control: &PgPool,
    account: Uuid,
    org: &str,
    member: Uuid,
) -> Result<Option<String>, String> {
    use crate::connectors::{MicrosoftConnector, TokenOwner};
    let dek = crate::crypto::DekPlane::resolve_for_org(state, control, account, Some(org))
        .await
        .map_err(|e| e.to_string())?;
    let Some(mut creds) = crate::routes::connectors::load_creds_bg(
        control,
        &dek,
        account,
        "microsoft",
        TokenOwner::Member(member),
    )
    .await
    .map_err(|e| e.to_string())?
    else {
        return Ok(None);
    };
    MicrosoftConnector
        .refresh_if_needed(&state.http_client, &mut creds)
        .await
        .map_err(|e| e.to_string())?;
    if let Some(bundle) = creds.refreshed.as_ref() {
        crate::routes::oauth::store_token_bundle(control, &dek, account, "microsoft", TokenOwner::Member(member), bundle)
            .await
            .map_err(|r| format!("Microsoft credential persistence failed ({})", r.status()))?;
    }
    Ok(Some(creds.access_token))
}

/// Called from `teams_meeting_sync::tick` once per tenant. Never fails the tick: a
/// problem with scheduling must not stop already-authorized captures from polling.
/// Returns how soon the earliest remaining intent is due, so the tick can wake for it.
pub(crate) async fn run_tick(
    state: &std::sync::Arc<crate::AppState>,
    control: &PgPool,
    pool: &PgPool,
    account: Uuid,
    org: &str,
    owners: &[Uuid],
) -> Option<std::time::Duration> {
    let mut wake: Option<std::time::Duration> = None;
    let mut merge = |d: Option<std::time::Duration>| {
        if let Some(d) = d {
            wake = Some(wake.map_or(d, |w| w.min(d)));
        }
    };
    // Phase 1 (cheap, DB only): claim every eligible member's due intents. The claim also
    // expires overdue `pending_save` rows, so expiry never waits behind a slow member.
    let mut work: Vec<(Uuid, String, Vec<Intent>)> = Vec::new();
    for &member in owners {
        if !matches!(crate::middleware::auth::live_org_membership_for_capture(control, org, member).await, Ok(Some((live, _))) if live == account) {
            continue;
        }
        let Ok(Some((tenant, aad))) = crate::routes::teams_onboarding::identity(control, account, member).await else {
            continue;
        };
        match claim(pool, account, member, &tenant, &aad).await {
            Ok(v) if !v.is_empty() => work.push((member, tenant, v)),
            Ok(_) => {}
            Err(error) => tracing::warn!(%error, "outlook schedule claim failed"),
        }
    }
    // Phase 2 (Graph): fair order across ticks, and a budget per member, so one throttled
    // member cannot use up the whole tick for everyone after it.
    static ROTATION: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let offset = ROTATION.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    for (member, tenant, intents) in rotate(work, offset) {
        match member_token(state, control, account, org, member).await {
            Ok(Some(token)) => {
                let env = Env {
                    http: &state.http_client,
                    graph: teams::GRAPH,
                    pool,
                    token: &token,
                    now: Utc::now(),
                };
                let run = crate::graph_rate_limit::scoped(
                    crate::graph_rate_limit::rate_key(account, &tenant),
                    run_member(&env, intents),
                );
                if tokio::time::timeout(MEMBER_BUDGET, run).await.is_err() {
                    // Claimed intents keep their lease and are retried when it lapses.
                    tracing::warn!("outlook schedule member pass timed out");
                }
            }
            Ok(None) => {}
            Err(error) => tracing::warn!(%error, "outlook schedule token unavailable"),
        }
    }
    for &member in owners {
        if let Ok(next) = svc::next_delay(pool, account, member).await {
            merge(next);
        }
    }
    wake
}

/// Time one member's Graph work may take within a tick (the tick's total is 60 s).
const MEMBER_BUDGET: std::time::Duration = std::time::Duration::from_secs(15);

/// Start the list `by` positions further along (wrapping), so the first member differs per tick.
fn rotate<T>(mut v: Vec<T>, by: usize) -> Vec<T> {
    if !v.is_empty() {
        let k = by % v.len();
        v.rotate_left(k);
    }
    v
}

/// Live-Postgres suite with a local Graph fixture. Gated on `ORGT_TEST_BASE_URL` (a
/// connection with CREATE DATABASE rights); WITHOUT it every test prints a skip line and
/// passes vacuously, so check the output for `skipping`.
///
///   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
///     cargo test -p coralswarm-backend --lib outlook_schedule_pg_tests -- --test-threads=1 --nocapture
///
/// The database role under test is the production tenant role (`SET ROLE
/// coralswarm_tenant_app`), so RLS and grants are the real ones. Time-dependent
/// behaviour is driven through `Env::now`; DB-side `now()` is real.
#[cfg(test)]
mod outlook_schedule_pg_tests {
    use super::*;
    use crate::connectors::outlook_schedule::PROPERTY;
    use axum::{
        body::Body,
        extract::State,
        http::{Request as HttpRequest, StatusCode},
        response::IntoResponse,
        Json, Router,
    };
    use serde_json::{json, Value};
    use sqlx::{postgres::PgPoolOptions, Connection, Executor, PgConnection};
    use std::collections::{HashMap, HashSet};
    use std::sync::{Arc, Mutex};

    const LINK_A: &str = "https://teams.microsoft.com/l/meetup-join/AAA?context=x";
    const LINK_B: &str = "https://teams.microsoft.com/l/meetup-join/BBB?context=x";

    // ── Local Graph ─────────────────────────────────────────────────────────
    #[derive(Default)]
    struct G {
        events: HashMap<String, Value>,
        discovery: HashMap<String, String>, // nonce -> event id
        duplicate_discovery: bool,
        /// Nonce discovery answers 500 (a transient provider failure).
        discovery_fails: bool,
        /// Other tagged events Graph returns whatever the `contains` filter says.
        foreign: Vec<Value>,
        instances: HashMap<String, Vec<Value>>,
        meetings: HashMap<String, Value>, // joinUrl -> onlineMeeting
        gone: HashSet<String>,
        patch_status: u16,
        patches: Vec<(String, Value)>,
        calls: Vec<String>,
    }
    type Shared = Arc<Mutex<G>>;

    async fn reply(State(g): State<Shared>, request: HttpRequest<Body>) -> axum::response::Response {
        let method = request.method().to_string();
        let uri = request.uri().clone();
        let url = reqwest::Url::parse(&format!("http://h{uri}")).unwrap();
        let q: HashMap<String, String> = url.query_pairs().into_owned().collect();
        let body = axum::body::to_bytes(request.into_body(), 1 << 20).await.unwrap();
        let path = url.path().strip_prefix("/v1.0").unwrap_or(url.path()).to_owned();
        let mut g = g.lock().unwrap();
        g.calls.push(format!("{method} {path}"));
        match (method.as_str(), path.as_str()) {
            ("GET", "/me/events") if g.discovery_fails => {
                (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error":{"code":"boom"}}))).into_response()
            }
            ("GET", "/me/events") => {
                let filter = q.get("$filter").cloned().unwrap_or_default();
                let gone = g.gone.clone();
                let hit = g.discovery.iter().filter(|(_, id)| !gone.contains(*id)).find(|(nonce, _)| filter.contains(nonce.as_str())).map(|(n, id)| (n.clone(), id.clone()));
                let mut rows = g.foreign.clone();
                if let Some((nonce, id)) = hit {
                    let mut row = g.events[&id].clone();
                    row["singleValueExtendedProperties"] = json!([{"id":PROPERTY.to_ascii_lowercase(),"value":json!({"coralswarmCaptureIntent":nonce,"coralswarmCaptureIntentId":Uuid::new_v4()}).to_string()}]);
                    rows.push(row.clone());
                    if g.duplicate_discovery {
                        let mut other = row.clone();
                        other["id"] = json!("DUP");
                        rows.push(other);
                    }
                }
                Json(json!({"value":rows})).into_response()
            }
            ("GET", "/me/onlineMeetings") => {
                let filter = q.get("$filter").cloned().unwrap_or_default();
                let link = filter.split('\'').nth(1).unwrap_or_default().to_owned();
                match g.meetings.get(&link) {
                    Some(m) => Json(json!({"value":[m]})).into_response(),
                    None => Json(json!({"value":[]})).into_response(),
                }
            }
            ("PATCH", p) if p.starts_with("/me/onlineMeetings/") => {
                let payload: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                g.patches.push((p.to_owned(), payload));
                let status = if g.patch_status == 0 { 200 } else { g.patch_status };
                (StatusCode::from_u16(status).unwrap(), Json(json!({}))).into_response()
            }
            ("GET", p) if p.starts_with("/me/events/") => {
                let rest = &p["/me/events/".len()..];
                if let Some(id) = rest.strip_suffix("/instances") {
                    return Json(json!({"value": g.instances.get(id).cloned().unwrap_or_default()})).into_response();
                }
                if g.gone.contains(rest) || !g.events.contains_key(rest) {
                    return (StatusCode::NOT_FOUND, Json(json!({"error":{"code":"ErrorItemNotFound"}}))).into_response();
                }
                Json(g.events[rest].clone()).into_response()
            }
            _ => (StatusCode::NOT_FOUND, Json(json!({"error":{"code":"unrouted"}}))).into_response(),
        }
    }

    fn dt(t: DateTime<Utc>) -> Value {
        json!({"dateTime": t.to_rfc3339(), "timeZone": "UTC"})
    }
    fn ev(id: &str, kind: &str, start: DateTime<Utc>, end: DateTime<Utc>, join: Option<&str>) -> Value {
        let mut v = json!({"id":id,"type":kind,"isOrganizer":true,"isCancelled":false,"isAllDay":false,
            "start":dt(start),"end":dt(end),
            "onlineMeeting": join.map(|j| json!({"joinUrl": j}))});
        if kind == "seriesMaster" {
            v["recurrence"] = json!({"range":{"type":"noEnd"}});
        }
        v
    }
    fn inst(start: DateTime<Utc>, end: DateTime<Utc>, cancelled: bool, kind: &str) -> Value {
        json!({"id":"inst","type":kind,"isCancelled":cancelled,"start":dt(start),"end":dt(end)})
    }
    fn online(id: &str, link: &str, start: DateTime<Utc>, end: DateTime<Utc>) -> Value {
        json!({"id":id,"subject":"Planning","startDateTime":start.to_rfc3339(),"endDateTime":end.to_rfc3339(),
            "joinWebUrl":link,"recordAutomatically":false})
    }

    // ── World ───────────────────────────────────────────────────────────────
    struct World {
        base: String,
        db: crate::test_db::TestDatabase,
        sup: PgPool,
        pool: PgPool,
        g: Shared,
        graph_url: String,
        server: tokio::task::JoinHandle<()>,
        http: Client,
        account: Uuid,
        user: Uuid,
        graph: Uuid,
        tid: String,
        aad: String,
    }

    fn with_db(base: &str, db: &str) -> String {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{db}{params}")
    }

    macro_rules! world {
        ($name:expr) => {
            match World::new($name).await {
                Some(w) => w,
                None => {
                    eprintln!("skipping {}: set ORGT_TEST_BASE_URL", $name);
                    return;
                }
            }
        };
    }

    impl World {
        async fn new(name: &str) -> Option<World> {
            let base = std::env::var("ORGT_TEST_BASE_URL").ok()?;
            let db = crate::test_db::TestDatabase::from_url(&base, &format!("outlook_sched_{}", name.replace('-', "_")));
            let mut admin = PgConnection::connect(&base).await.expect("admin");
            admin.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.unwrap();
            admin.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
            let sup = PgPoolOptions::new().max_connections(4).connect(&with_db(&base, &db)).await.unwrap();
            sqlx::migrate!("./migrations_tenant").run(&sup).await.expect("tenant migrations");
            let pool = PgPoolOptions::new()
                .max_connections(6)
                .after_connect(|conn, _| {
                    Box::pin(async move {
                        conn.execute("SET ROLE coralswarm_tenant_app").await?;
                        Ok(())
                    })
                })
                .connect(&with_db(&base, &db))
                .await
                .unwrap();
            let (account, user, graph) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
            sqlx::query("INSERT INTO graphs(id,account_id,owner_user_id,kind,name) VALUES($1,$2,$3,'member','Private')")
                .bind(graph).bind(account).bind(user)
                .execute(&sup).await.unwrap();
            let g: Shared = Arc::new(Mutex::new(G::default()));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let graph_url = format!("http://{}/v1.0", listener.local_addr().unwrap());
            let app = Router::new().fallback(reply).with_state(g.clone());
            let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap(); });
            Some(World {
                base, db, sup, pool, g, graph_url, server,
                http: Client::new(), account, user, graph,
                tid: Uuid::new_v4().to_string(), aad: Uuid::new_v4().to_string(),
            })
        }

        async fn finish(self) {
            self.server.abort();
            self.pool.close().await;
            self.sup.close().await;
            if let Ok(mut admin) = PgConnection::connect(&self.base).await {
                let _ = admin.execute(format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", self.db).as_str()).await;
            }
        }

        fn env_at(&self, now: DateTime<Utc>) -> Env<'_> {
            Env { http: &self.http, graph: &self.graph_url, pool: &self.pool, token: "tok", now }
        }
        fn env(&self) -> Env<'_> {
            self.env_at(Utc::now())
        }

        async fn new_intent(&self) -> Intent {
            let c = svc::create(&self.pool, self.account, self.user, &self.tid, &self.aad, self.graph)
                .await
                .ok()
                .expect("create intent");
            self.reload(c.id).await
        }
        async fn reload(&self, id: Uuid) -> Intent {
            svc::get(&self.pool, self.account, self.user, id).await.unwrap().expect("intent")
        }
        /// Process the intent as the worker would, from its current stored state.
        async fn pass(&self, id: Uuid) -> Intent {
            self.pass_at(id, Utc::now()).await
        }
        async fn pass_at(&self, id: Uuid, now: DateTime<Utc>) -> Intent {
            let i = self.reload(id).await;
            process(&self.env_at(now), &i).await.expect("process");
            self.reload(id).await
        }

        /// Publish a saved event for this intent (the user pressed Save).
        fn save(&self, i: &Intent, event: Value) {
            let id = event["id"].as_str().unwrap().to_owned();
            let mut g = self.g.lock().unwrap();
            g.events.insert(id.clone(), event);
            g.discovery.insert(i.nonce.to_string(), id);
        }
        fn meeting(&self, id: &str, link: &str, start: DateTime<Utc>, end: DateTime<Utc>) {
            self.g.lock().unwrap().meetings.insert(link.to_owned(), online(id, link, start, end));
        }
        fn edit(&self, id: &str, f: impl FnOnce(&mut Value)) {
            f(self.g.lock().unwrap().events.get_mut(id).unwrap());
        }

        async fn capture(&self, meeting: &str) -> Option<Cap> {
            sqlx::query_as::<_, Cap>(
                "SELECT id, revoked_at IS NOT NULL AS revoked, occurrence_start, occurrence_end, transcript_end, capture_generation, last_stopped_at IS NOT NULL AS barrier, aad_id
                 FROM teams_meeting_captures WHERE account_id=$1 AND user_id=$2 AND meeting_id=$3",
            )
            .bind(self.account).bind(self.user).bind(meeting)
            .fetch_optional(&self.sup).await.unwrap()
        }
        /// Pretend `checks` consecutive 404s began `minutes` ago.
        async fn age_gone(&self, id: Uuid, checks: i32, minutes: i32) {
            sqlx::query("UPDATE outlook_schedule_intents SET gone_checks=$2,gone_since=now()-make_interval(mins=>$3) WHERE id=$1")
                .bind(id).bind(checks).bind(minutes).execute(&self.sup).await.unwrap();
        }
        async fn gone_state(&self, id: Uuid) -> (i32, bool, DateTime<Utc>) {
            sqlx::query_as("SELECT gone_checks, gone_since IS NOT NULL, next_check_at FROM outlook_schedule_intents WHERE id=$1")
                .bind(id).fetch_one(&self.sup).await.unwrap()
        }
        fn patches(&self) -> Vec<(String, Value)> {
            self.g.lock().unwrap().patches.clone()
        }
    }

    #[derive(sqlx::FromRow, Debug)]
    struct Cap {
        id: Uuid,
        revoked: bool,
        occurrence_start: DateTime<Utc>,
        occurrence_end: DateTime<Utc>,
        transcript_end: Option<DateTime<Utc>>,
        capture_generation: Uuid,
        barrier: bool,
        aad_id: String,
    }

    fn us(t: DateTime<Utc>) -> i64 {
        t.timestamp_micros()
    }
    fn hours(h: i64) -> Duration {
        Duration::hours(h)
    }

    /// One-off future meeting: activates on Save, window = the event, and
    /// transcription is reported `manual` (no reason) with NO PATCH (#1119) and no chat reads.
    #[tokio::test]
    async fn one_off_activates_on_save_reports_manual_and_never_patches_or_hits_the_recording_gate() {
        let w = world!("one_off");
        use chrono::Timelike;
        // Graph may return finer fractions than SQLx/Postgres can store.
        // Pin the residue so a repeated unchanged poll cannot pass by chance.
        let start = (Utc::now() + hours(24)).with_nanosecond(123_456_789).unwrap();
        let end = start + hours(1);
        let stored: DateTime<Utc> = sqlx::query_scalar("SELECT $1::timestamptz")
            .bind(start).fetch_one(&w.sup).await.unwrap();
        assert_ne!(stored, start, "the fixture must exercise lost nanosecond precision");
        assert_eq!(stored.timestamp_micros(), start.timestamp_micros());

        w.meeting("MEET-A", LINK_A, start, end);
        let i = w.new_intent().await;
        assert_eq!(i.status, "pending_save");

        // Not saved yet: still waiting, nothing created.
        let i = w.pass(i.id).await;
        assert_eq!(i.status, "pending_save");
        assert!(w.capture("MEET-A").await.is_none());

        w.save(&i, ev("EV1", "singleInstance", start, end, Some(LINK_A)));
        let i = w.pass(i.id).await;
        assert_eq!(i.status, "active", "{i:?}");
        assert!(!i.recurring);
        assert_eq!(i.event_id.as_deref(), Some("EV1"));
        let cap = w.capture("MEET-A").await.expect("capture");
        assert!(!cap.revoked);
        assert_eq!((us(cap.occurrence_start), us(cap.occurrence_end)), (us(start), us(end)));
        assert_eq!(cap.transcript_end, None);
        assert_eq!(cap.aad_id, w.aad);
        assert_eq!(i.capture_id, Some(cap.id));
        assert_eq!(i.auto_transcription.as_deref(), Some("manual"));
        assert_eq!(i.manual_reason, None);
        assert!(w.patches().is_empty(), "recordAutomatically is never changed");
        // The Teams-pane recording gate (chat reads) is never touched: a scheduled meeting
        // cannot be recording yet and the follower sends no chat id.
        let calls = w.g.lock().unwrap().calls.clone();
        assert!(calls.iter().all(|c| !c.contains("/chats") && !c.contains("/messages")), "{calls:?}");
        let view = i.view();
        assert_eq!(view["status"], "active");
        assert_eq!(view["next_occurrence_start"], json!(i.occurrence_start));
        assert_eq!(view["auto_transcription"], "manual");
        assert_eq!(view["manual_reason"], serde_json::Value::Null);
        // A second poll with nothing changed is a no-op (no second PATCH, same generation).
        let before = w.capture("MEET-A").await.unwrap().capture_generation;
        let audits_before: i64 = sqlx::query_scalar("SELECT count(*) FROM connector_audit_log")
            .fetch_one(&w.sup).await.unwrap();

        let i = w.pass(i.id).await;
        assert_eq!(i.status, "active");
        assert_eq!(w.capture("MEET-A").await.unwrap().capture_generation, before);
        let audits_after: i64 = sqlx::query_scalar("SELECT count(*) FROM connector_audit_log")
            .fetch_one(&w.sup).await.unwrap();
        assert_eq!(audits_after, audits_before, "unchanged polls must not invent fresh consent");
        // A real change at storage precision must still rotate the generation.
        w.edit("EV1", |e| {
            e["start"] = dt(start + Duration::microseconds(1));
            e["end"] = dt(end + Duration::microseconds(1));
        });
        w.pass(i.id).await;
        assert_ne!(w.capture("MEET-A").await.unwrap().capture_generation, before,
            "a genuine one-microsecond reschedule must get a new generation");

        assert!(w.patches().is_empty());
        w.finish().await;
    }

    /// (1) Consent is the EVENT: a move updates the capture window in place.
    #[tokio::test]
    async fn follows_a_move() {
        let w = world!("move");
        let (start, end) = (Utc::now() + hours(24), Utc::now() + hours(25));
        w.meeting("MEET-A", LINK_A, start, end);
        let i = w.new_intent().await;
        w.save(&i, ev("EV1", "singleInstance", start, end, Some(LINK_A)));
        let i = w.pass(i.id).await;
        let first = w.capture("MEET-A").await.unwrap();

        let (nstart, nend) = (start + hours(72), end + hours(73));
        w.edit("EV1", |e| {
            e["start"] = dt(nstart);
            e["end"] = dt(nend);
        });
        let i = w.pass(i.id).await;
        let moved = w.capture("MEET-A").await.unwrap();
        assert_eq!(moved.id, first.id, "same capture row, updated in place");
        assert!(!moved.revoked);
        assert_eq!((us(moved.occurrence_start), us(moved.occurrence_end)), (us(nstart), us(nend)));
        assert_ne!(moved.capture_generation, first.capture_generation, "fresh generation for the new window");
        assert_eq!(i.status, "active");
        assert_eq!(us(i.occurrence_start.unwrap()), us(nstart));
        assert_eq!(i.view()["next_occurrence_start"], json!(i.occurrence_start));
        assert!(w.patches().is_empty());
        w.finish().await;
    }

    /// (1) A changed Teams link is followed: new online meeting captured, old one revoked.
    #[tokio::test]
    async fn follows_a_join_url_change() {
        let w = world!("joinurl");
        let (start, end) = (Utc::now() + hours(24), Utc::now() + hours(25));
        w.meeting("MEET-A", LINK_A, start, end);
        w.meeting("MEET-B", LINK_B, start, end);
        let i = w.new_intent().await;
        w.save(&i, ev("EV1", "singleInstance", start, end, Some(LINK_A)));
        let i = w.pass(i.id).await;
        let a = w.capture("MEET-A").await.unwrap();
        assert_eq!(i.capture_id, Some(a.id));

        w.edit("EV1", |e| e["onlineMeeting"] = json!({"joinUrl": LINK_B}));
        let i = w.pass(i.id).await;
        let (a2, b) = (w.capture("MEET-A").await.unwrap(), w.capture("MEET-B").await.unwrap());
        assert!(a2.revoked, "the old meeting is no longer captured");
        assert!(!b.revoked);
        assert_eq!(i.meeting_id.as_deref(), Some("MEET-B"));
        assert_eq!(i.join_url.as_deref(), Some(LINK_B));
        assert_eq!(i.capture_id, Some(b.id));
        assert_eq!(i.status, "active");
        assert_eq!(i.auto_transcription.as_deref(), Some("manual"));
        assert!(w.patches().is_empty());
        w.finish().await;
    }

    #[tokio::test]
    async fn cancelled_or_deleted_event_stops_capture() {
        for deleted in [false, true] {
            let w = world!("cancel");
            let (start, end) = (Utc::now() + hours(24), Utc::now() + hours(25));
            w.meeting("MEET-A", LINK_A, start, end);
            let i = w.new_intent().await;
            w.save(&i, ev("EV1", "singleInstance", start, end, Some(LINK_A)));
            let i = w.pass(i.id).await;
            assert_eq!(i.status, "active");
            if deleted {
                w.g.lock().unwrap().gone.insert("EV1".into());
                // A 404 alone is not enough; once it has persisted long enough it is.
                let i = w.pass(i.id).await;
                assert_eq!(i.status, "active");
                w.age_gone(i.id, 2, 10).await;
            } else {
                w.edit("EV1", |e| e["isCancelled"] = json!(true));
            }
            let i = w.pass(i.id).await;
            assert_eq!(i.status, "stopped");
            assert_eq!(i.error_code.as_deref(), Some(if deleted { "event_deleted" } else { "event_cancelled" }));
            assert!(w.capture("MEET-A").await.unwrap().revoked);
            // Terminal: later polls never reopen it.
            w.g.lock().unwrap().gone.clear();
            w.edit("EV1", |e| e["isCancelled"] = json!(false));
            let i = w.pass(i.id).await;
            assert_eq!(i.status, "stopped");
            assert!(w.capture("MEET-A").await.unwrap().revoked);
            w.finish().await;
        }
    }

    /// (2) Recurring: next occurrence scheduled, moved exception used as-is, cancelled
    /// skipped, rotation as occurrences pass, and `recordAutomatically` is never PATCHed.
    #[tokio::test]
    async fn recurring_schedules_occurrences_honours_exception_skips_cancelled_never_patches() {
        let w = world!("recurring");
        let now = Utc::now();
        let (s0, e0) = (now - hours(1), now + hours(1));
        w.meeting("MEET-S", LINK_A, s0, e0);
        let i = w.new_intent().await;
        w.save(&i, ev("SERIES", "seriesMaster", s0, e0, Some(LINK_A)));
        let cancelled = (now + hours(1), now + hours(2));
        let moved = (now + hours(30), now + hours(31)); // an exception moved off its slot
        let later = (now + hours(54), now + hours(55));
        w.g.lock().unwrap().instances.insert(
            "SERIES".into(),
            vec![
                inst(cancelled.0, cancelled.1, true, "occurrence"),
                inst(moved.0, moved.1, false, "exception"),
                inst(later.0, later.1, false, "occurrence"),
            ],
        );
        let i = w.pass(i.id).await;
        assert_eq!(i.status, "active", "{i:?}");
        assert!(i.recurring);
        let cap = w.capture("MEET-S").await.unwrap();
        assert_eq!((us(cap.occurrence_start), us(cap.occurrence_end)), (us(moved.0), us(moved.1)), "cancelled skipped, exception honoured");
        assert_eq!(cap.transcript_end.map(us), Some(us(moved.1 + crate::connectors::teams_targeted_transcripts::SERIES_OVERRUN)));
        assert_eq!(i.auto_transcription.as_deref(), Some("manual"));
        assert_eq!(i.manual_reason.as_deref(), Some("recurring"));
        assert_eq!(i.view()["recurring"], true);
        assert_eq!(i.view()["next_occurrence_start"], json!(i.occurrence_start));
        assert!(w.patches().is_empty(), "never PATCH recordAutomatically on a series");
        let gen_before = cap.capture_generation;

        // The exception moves again: followed.
        let moved2 = (now + hours(32), now + hours(33));
        w.g.lock().unwrap().instances.insert(
            "SERIES".into(),
            vec![inst(moved2.0, moved2.1, false, "exception"), inst(later.0, later.1, false, "occurrence")],
        );
        let i = w.pass(i.id).await;
        let cap = w.capture("MEET-S").await.unwrap();
        assert_eq!(us(cap.occurrence_start), us(moved2.0));
        assert_ne!(cap.capture_generation, gen_before);

        // 40 hours later that occurrence's transcript window is closed: rotate to the next.
        let i = w.pass_at(i.id, now + hours(40)).await;
        let rotated = w.capture("MEET-S").await.unwrap();
        assert_eq!(rotated.id, cap.id, "one row per online meeting, rotated in place");
        assert_eq!((us(rotated.occurrence_start), us(rotated.occurrence_end)), (us(later.0), us(later.1)));
        assert!(!rotated.revoked);
        assert_eq!(i.status, "active");
        assert!(w.patches().is_empty());
        w.finish().await;
    }

    #[tokio::test]
    async fn recurring_reports_recurring_even_if_record_automatically_is_already_on() {
        let w = world!("recurring_enabled");
        let now = Utc::now();
        let (s, e) = (now + hours(24), now + hours(25));
        let mut m = online("MEET-S", LINK_A, s, e);
        m["recordAutomatically"] = json!(true);
        w.g.lock().unwrap().meetings.insert(LINK_A.into(), m);
        let i = w.new_intent().await;
        w.save(&i, ev("SERIES", "seriesMaster", s, e, Some(LINK_A)));
        w.g.lock().unwrap().instances.insert("SERIES".into(), vec![inst(s, e, false, "occurrence")]);
        let i = w.pass(i.id).await;
        assert_eq!(i.auto_transcription.as_deref(), Some("manual"));
        assert_eq!(i.manual_reason.as_deref(), Some("recurring"));
        assert!(w.patches().is_empty());
        w.finish().await;
    }

    #[tokio::test]
    async fn bounded_series_that_has_ended_stops_and_cancelled_series_stops() {
        let w = world!("series_end");
        let now = Utc::now();
        let (s, e) = (now + hours(24), now + hours(25));
        w.meeting("MEET-S", LINK_A, s, e);
        let i = w.new_intent().await;
        w.save(&i, ev("SERIES", "seriesMaster", s, e, Some(LINK_A)));
        w.g.lock().unwrap().instances.insert("SERIES".into(), vec![inst(s, e, false, "occurrence")]);
        let i = w.pass(i.id).await;
        assert_eq!(i.status, "active");
        // Range ends and no occurrence is left.
        w.edit("SERIES", |v| v["recurrence"] = json!({"range":{"type":"endDate"}}));
        w.g.lock().unwrap().instances.insert("SERIES".into(), vec![]);
        let i = w.pass(i.id).await;
        assert_eq!((i.status.as_str(), i.error_code.as_deref()), ("stopped", Some("series_ended")));
        assert!(w.capture("MEET-S").await.unwrap().revoked);
        w.finish().await;

        let w = world!("series_cancel");
        w.meeting("MEET-S", LINK_A, s, e);
        let i = w.new_intent().await;
        w.save(&i, ev("SERIES", "seriesMaster", s, e, Some(LINK_A)));
        w.g.lock().unwrap().instances.insert("SERIES".into(), vec![inst(s, e, false, "occurrence")]);
        let i = w.pass(i.id).await;
        w.edit("SERIES", |v| v["isCancelled"] = json!(true));
        let i = w.pass(i.id).await;
        assert_eq!((i.status.as_str(), i.error_code.as_deref()), ("stopped", Some("event_cancelled")));
        w.finish().await;
    }

    /// (1) A move can never resurrect a Stop, whichever way the Stop happened.
    #[tokio::test]
    async fn a_move_never_resurrects_a_stop() {
        // a) The user stops the intent while a worker pass already holds a stale copy.
        let w = world!("stop_a");
        let (start, end) = (Utc::now() + hours(24), Utc::now() + hours(25));
        w.meeting("MEET-A", LINK_A, start, end);
        let i = w.new_intent().await;
        w.save(&i, ev("EV1", "singleInstance", start, end, Some(LINK_A)));
        let active = w.pass(i.id).await;
        let stale = active.clone();
        assert!(svc::stop_by_user(&w.pool, w.account, w.user, i.id).await.unwrap());
        let stopped_cap = w.capture("MEET-A").await.unwrap();
        assert!(stopped_cap.revoked && stopped_cap.barrier, "Stop revokes and records the barrier");
        w.edit("EV1", |e| {
            e["start"] = dt(start + hours(48));
            e["end"] = dt(end + hours(48));
        });
        process(&w.env(), &stale).await.unwrap();
        let after = w.capture("MEET-A").await.unwrap();
        assert!(after.revoked, "stale pass must not revive");
        assert_eq!(us(after.occurrence_start), us(start), "window untouched");
        assert_eq!(w.reload(i.id).await.status, "stopped");
        // And a fresh pass never even claims a stopped intent.
        assert!(svc::claim_due(&w.pool, w.account, w.user, &w.tid, &w.aad).await.unwrap().is_empty());
        assert_eq!(svc::stop_by_user(&w.pool, w.account, w.user, i.id).await.unwrap(), true, "idempotent");
        w.finish().await;

        // b) Stop through the Teams pane / capture revoke: the follower treats it as the user's Stop.
        let w = world!("stop_b");
        w.meeting("MEET-A", LINK_A, start, end);
        let i = w.new_intent().await;
        w.save(&i, ev("EV1", "singleInstance", start, end, Some(LINK_A)));
        let i = w.pass(i.id).await;
        sqlx::query("UPDATE teams_meeting_captures SET revoked_at=now() WHERE meeting_id='MEET-A'")
            .execute(&w.sup).await.unwrap();
        w.edit("EV1", |e| {
            e["start"] = dt(start + hours(48));
            e["end"] = dt(end + hours(48));
        });
        let i = w.pass(i.id).await;
        assert_eq!((i.status.as_str(), i.error_code.as_deref()), ("stopped", Some("capture_stopped")));
        let cap = w.capture("MEET-A").await.unwrap();
        assert!(cap.revoked);
        assert_eq!(us(cap.occurrence_start), us(start));
        w.finish().await;

        // c) Recurring: Stop, then the next occurrence comes round.
        let w = world!("stop_c");
        let now = Utc::now();
        w.meeting("MEET-S", LINK_A, now, now + hours(1));
        let i = w.new_intent().await;
        w.save(&i, ev("SERIES", "seriesMaster", now, now + hours(1), Some(LINK_A)));
        let o1 = (now + hours(2), now + hours(3));
        let o2 = (now + hours(50), now + hours(51));
        w.g.lock().unwrap().instances.insert("SERIES".into(), vec![inst(o1.0, o1.1, false, "occurrence"), inst(o2.0, o2.1, false, "occurrence")]);
        let i = w.pass(i.id).await;
        let stale = w.reload(i.id).await;
        svc::stop_by_user(&w.pool, w.account, w.user, i.id).await.unwrap();
        process(&w.env_at(now + hours(40)), &stale).await.unwrap();
        let cap = w.capture("MEET-S").await.unwrap();
        assert!(cap.revoked);
        assert_eq!(us(cap.occurrence_start), us(o1.0), "not rotated after Stop");
        w.finish().await;

        // d) Stopped before the event was even saved: Save later creates nothing.
        let w = world!("stop_d");
        w.meeting("MEET-A", LINK_A, start, end);
        let i = w.new_intent().await;
        let stale = i.clone();
        svc::stop_by_user(&w.pool, w.account, w.user, i.id).await.unwrap();
        w.save(&i, ev("EV1", "singleInstance", start, end, Some(LINK_A)));
        process(&w.env(), &stale).await.unwrap();
        assert!(w.capture("MEET-A").await.is_none());
        assert_eq!(w.reload(i.id).await.status, "stopped");
        w.finish().await;
    }

    /// A newer deliberate intent may re-enable a Stopped capture; an older one may not.
    #[tokio::test]
    async fn stop_barrier_only_yields_to_a_later_intent() {
        let w = world!("barrier");
        let (start, end) = (Utc::now() + hours(24), Utc::now() + hours(25));
        w.meeting("MEET-A", LINK_A, start, end);
        let old = w.new_intent().await; // created BEFORE the stop
        let first = w.new_intent().await;
        w.save(&first, ev("EV1", "singleInstance", start, end, Some(LINK_A)));
        let first = w.pass(first.id).await;
        svc::stop_by_user(&w.pool, w.account, w.user, first.id).await.unwrap();
        let barrier = w.capture("MEET-A").await.unwrap();
        assert!(barrier.revoked);

        // The older intent finds the same event (re-tagged): still blocked by the barrier.
        w.save(&old, ev("EV1", "singleInstance", start, end, Some(LINK_A)));
        let old = w.pass(old.id).await;
        assert_eq!((old.status.as_str(), old.error_code.as_deref()), ("stopped", Some("capture_stopped")));
        assert!(w.capture("MEET-A").await.unwrap().revoked);

        // A brand-new deliberate intent (created after the stop) may capture it again.
        let newer = w.new_intent().await;
        w.save(&newer, ev("EV1", "singleInstance", start, end, Some(LINK_A)));
        let newer = w.pass(newer.id).await;
        assert_eq!(newer.status, "active", "{newer:?}");
        let cap = w.capture("MEET-A").await.unwrap();
        assert!(!cap.revoked);
        assert!(cap.barrier, "the barrier column is retained after re-enable");
        assert_eq!(cap.id, barrier.id);
        w.finish().await;
    }

    #[tokio::test]
    async fn refuses_non_organizer_ambiguity_ended_and_waits_for_a_teams_link() {
        let w = world!("guards");
        let (start, end) = (Utc::now() + hours(24), Utc::now() + hours(25));
        w.meeting("MEET-A", LINK_A, start, end);

        let i = w.new_intent().await;
        let mut e = ev("EV1", "singleInstance", start, end, Some(LINK_A));
        e["isOrganizer"] = json!(false);
        w.save(&i, e);
        let i = w.pass(i.id).await;
        assert_eq!((i.status.as_str(), i.error_code.as_deref()), ("error", Some("not_organizer")));
        assert!(w.capture("MEET-A").await.is_none());

        let i = w.new_intent().await;
        w.save(&i, ev("EV2", "singleInstance", start, end, Some(LINK_A)));
        w.g.lock().unwrap().duplicate_discovery = true;
        let i = w.pass(i.id).await;
        assert_eq!((i.status.as_str(), i.error_code.as_deref()), ("error", Some("ambiguous_nonce")));
        w.g.lock().unwrap().duplicate_discovery = false;

        let i = w.new_intent().await;
        w.save(&i, ev("EV3", "singleInstance", start - hours(100), end - hours(100), Some(LINK_A)));
        let i = w.pass(i.id).await;
        assert_eq!((i.status.as_str(), i.error_code.as_deref()), ("error", Some("already_ended")));
        assert!(w.capture("MEET-A").await.is_none());

        // Saved without a Teams meeting yet: stay pending (visible), then activate when added.
        let i = w.new_intent().await;
        w.save(&i, ev("EV4", "singleInstance", start, end, None));
        let i = w.pass(i.id).await;
        assert_eq!((i.status.as_str(), i.error_code.as_deref()), ("pending_save", Some("teams_link_missing")));
        w.edit("EV4", |e| e["onlineMeeting"] = json!({"joinUrl": LINK_A}));
        let i = w.pass(i.id).await;
        assert_eq!((i.status.as_str(), i.error_code.as_deref()), ("active", None));

        // 24h lifetime.
        let i = w.new_intent().await;
        sqlx::query("UPDATE outlook_schedule_intents SET expires_at=now()-interval '1 minute' WHERE id=$1")
            .bind(i.id).execute(&w.sup).await.unwrap();
        w.save(&i, ev("EV5", "singleInstance", start, end, Some(LINK_B)));
        let i = w.pass(i.id).await;
        assert_eq!(i.status, "expired");
        w.finish().await;
    }

    /// Tagging the same event again hands it to the newest intent (one capture row).
    #[tokio::test]
    async fn the_newest_intent_for_an_event_supersedes_the_older() {
        let w = world!("supersede");
        let (start, end) = (Utc::now() + hours(24), Utc::now() + hours(25));
        w.meeting("MEET-A", LINK_A, start, end);
        let a = w.new_intent().await;
        let b = w.new_intent().await;
        w.save(&a, ev("EV1", "singleInstance", start, end, Some(LINK_A)));
        let a = w.pass(a.id).await;
        assert_eq!(a.status, "active");
        w.save(&b, ev("EV1", "singleInstance", start, end, Some(LINK_A)));
        let b = w.pass(b.id).await;
        let a = w.reload(a.id).await;
        assert_eq!((a.status.as_str(), a.error_code.as_deref()), ("stopped", Some("superseded")));
        assert_eq!(b.status, "active");
        let cap = w.capture("MEET-A").await.unwrap();
        assert!(!cap.revoked, "the capture is handed over, not revoked");
        assert_eq!(b.capture_id, Some(cap.id));
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM teams_meeting_captures WHERE meeting_id='MEET-A'")
            .fetch_one(&w.sup).await.unwrap();
        assert_eq!(n, 1);
        // by_event returns the live one.
        let found = svc::by_event(&w.pool, w.account, w.user, "EV1").await.unwrap().unwrap();
        assert_eq!(found.id, b.id);
        w.finish().await;
    }

    /// Prod regression: another tagged event in the response must never end a pending intent.
    #[tokio::test]
    async fn a_foreign_nonce_in_the_response_leaves_the_intent_pending_and_ours_is_still_found() {
        let w = world!("foreign");
        let (start, end) = (Utc::now() + hours(24), Utc::now() + hours(25));
        w.meeting("MEET-A", LINK_A, start, end);
        let foreign = {
            let mut e = ev("OTHER", "singleInstance", start, end, Some(LINK_B));
            e["singleValueExtendedProperties"] = json!([{"id":PROPERTY,"value":json!({"coralswarmCaptureIntent":Uuid::new_v4()}).to_string()}]);
            e
        };
        w.g.lock().unwrap().foreign.push(foreign);
        let i = w.new_intent().await;
        let i = w.pass(i.id).await;
        assert_eq!((i.status.as_str(), i.error_code.as_deref()), ("pending_save", None));
        assert!(w.capture("MEET-A").await.is_none());
        // Saved: ours (lowercase property id, extra intent-id key) is picked out of the mix.
        w.save(&i, ev("EV1", "singleInstance", start, end, Some(LINK_A)));
        let i = w.pass(i.id).await;
        assert_eq!(i.status, "active", "{i:?}");
        assert_eq!(i.event_id.as_deref(), Some("EV1"));
        w.finish().await;
    }

    #[tokio::test]
    async fn one_404_keeps_the_capture_and_a_good_read_resets_the_count() {
        let w = world!("gone_once");
        let (start, end) = (Utc::now() + hours(24), Utc::now() + hours(25));
        w.meeting("MEET-A", LINK_A, start, end);
        let i = w.new_intent().await;
        w.save(&i, ev("EV1", "singleInstance", start, end, Some(LINK_A)));
        let i = w.pass(i.id).await;
        assert_eq!(i.status, "active");
        w.g.lock().unwrap().gone.insert("EV1".into());
        let i = w.pass(i.id).await;
        assert_eq!(i.status, "active", "a single 404 must not stop the capture");
        let (checks, since, next) = w.gone_state(i.id).await;
        assert_eq!((checks, since), (1, true));
        let soon = (next - Utc::now()).num_seconds();
        assert!((60..=130).contains(&soon), "retry in about 2 minutes, got {soon}s");
        assert!(!w.capture("MEET-A").await.unwrap().revoked);
        // Two more 404s that have NOT yet spanned the window are still unconfirmed.
        w.age_gone(i.id, 2, 1).await;
        assert_eq!(w.pass(i.id).await.status, "active");
        // The event answers again: the count is forgotten.
        w.g.lock().unwrap().gone.clear();
        let i = w.pass(i.id).await;
        assert_eq!(i.status, "active");
        assert_eq!(w.gone_state(i.id).await.0, 0);
        // And a fresh 404 starts from one again, not from the old count.
        w.g.lock().unwrap().gone.insert("EV1".into());
        let i = w.pass(i.id).await;
        assert_eq!((i.status.as_str(), w.gone_state(i.id).await.0), ("active", 1));
        w.finish().await;
    }

    #[tokio::test]
    async fn a_persistent_404_stops_after_enough_checks_over_enough_time() {
        let w = world!("gone_persist");
        let (start, end) = (Utc::now() + hours(24), Utc::now() + hours(25));
        w.meeting("MEET-A", LINK_A, start, end);
        let i = w.new_intent().await;
        w.save(&i, ev("EV1", "singleInstance", start, end, Some(LINK_A)));
        let i = w.pass(i.id).await;
        w.g.lock().unwrap().gone.insert("EV1".into());
        // Enough checks but not enough time: still tracking.
        w.age_gone(i.id, 5, 2).await;
        assert_eq!(w.pass(i.id).await.status, "active");
        // Enough time and checks: confirmed.
        w.age_gone(i.id, 2, 6).await;
        let i = w.pass(i.id).await;
        assert_eq!((i.status.as_str(), i.error_code.as_deref()), ("stopped", Some("event_deleted")));
        assert!(w.capture("MEET-A").await.unwrap().revoked);
        w.finish().await;
    }

    /// The event is re-created under a new id: found again by its nonce and adopted.
    #[tokio::test]
    async fn a_404_then_rediscovery_by_nonce_adopts_the_new_event_id() {
        let w = world!("gone_rediscover");
        let (start, end) = (Utc::now() + hours(24), Utc::now() + hours(25));
        w.meeting("MEET-A", LINK_A, start, end);
        let i = w.new_intent().await;
        w.save(&i, ev("EV1", "singleInstance", start, end, Some(LINK_A)));
        let i = w.pass(i.id).await;
        let cap = w.capture("MEET-A").await.unwrap();
        w.g.lock().unwrap().gone.insert("EV1".into());
        w.age_gone(i.id, 2, 10).await; // would be confirmed by the count alone
        w.save(&i, ev("EV2", "singleInstance", start + hours(1), end + hours(1), Some(LINK_A)));
        let i = w.pass(i.id).await;
        assert_eq!((i.status.as_str(), i.event_id.as_deref()), ("active", Some("EV2")));
        assert_eq!(w.gone_state(i.id).await.0, 0);
        let moved = w.capture("MEET-A").await.unwrap();
        assert!(!moved.revoked && moved.id == cap.id);
        assert_eq!(us(moved.occurrence_start), us(start + hours(1)));
        w.finish().await;
    }

    /// A 404 whose follow-up discovery FAILS is not evidence of deletion.
    #[tokio::test]
    async fn a_404_with_failing_discovery_never_counts_toward_deletion() {
        let w = world!("gone_discovery_fails");
        let (start, end) = (Utc::now() + hours(24), Utc::now() + hours(25));
        w.meeting("MEET-A", LINK_A, start, end);
        let i = w.new_intent().await;
        w.save(&i, ev("EV1", "singleInstance", start, end, Some(LINK_A)));
        let i = w.pass(i.id).await;
        w.g.lock().unwrap().gone.insert("EV1".into());
        w.g.lock().unwrap().discovery_fails = true;
        // Well past the 3-check / 5-minute window, repeatedly: still tracking, count untouched.
        w.age_gone(i.id, 0, 0).await;
        for _ in 0..6 {
            let i = w.pass(i.id).await;
            assert_eq!(i.status, "active");
            assert_eq!(w.gone_state(i.id).await.0, 0, "no 404 may be counted without definitive discovery");
        }
        assert!(!w.capture("MEET-A").await.unwrap().revoked);
        // Discovery recovers and finds nothing: counting starts and can then confirm.
        w.g.lock().unwrap().discovery_fails = false;
        let i = w.pass(i.id).await;
        assert_eq!((i.status.as_str(), w.gone_state(i.id).await.0), ("active", 1));
        w.age_gone(i.id, 2, 10).await;
        let i = w.pass(i.id).await;
        assert_eq!((i.status.as_str(), i.error_code.as_deref()), ("stopped", Some("event_deleted")));
        w.finish().await;
    }

    #[tokio::test]
    async fn a_404_never_resurrects_a_stop() {
        let w = world!("gone_stop");
        let (start, end) = (Utc::now() + hours(24), Utc::now() + hours(25));
        w.meeting("MEET-A", LINK_A, start, end);
        let i = w.new_intent().await;
        w.save(&i, ev("EV1", "singleInstance", start, end, Some(LINK_A)));
        let stale = w.pass(i.id).await;
        svc::stop_by_user(&w.pool, w.account, w.user, i.id).await.unwrap();
        w.g.lock().unwrap().gone.insert("EV1".into());
        // A stale pass (claimed before the Stop): neither counts a 404 nor adopts a new id.
        w.save(&i, ev("EV2", "singleInstance", start, end, Some(LINK_A)));
        process(&w.env(), &stale).await.unwrap();
        let after = w.reload(i.id).await;
        assert_eq!(after.status, "stopped");
        assert_eq!(after.gone_checks, 0);
        assert!(w.capture("MEET-A").await.unwrap().revoked);
        w.finish().await;
    }

    #[tokio::test]
    async fn status_view_carries_expires_at() {
        let w = world!("view_expiry");
        let i = w.new_intent().await;
        let v = i.view();
        assert_eq!(v["expires_at"], json!(i.expires_at));
        assert!(chrono::DateTime::parse_from_rfc3339(v["expires_at"].as_str().unwrap()).is_ok());
        w.finish().await;
    }

    #[tokio::test]
    async fn caps_isolation_claim_and_lease() {
        let w = world!("caps");
        let mut ids = Vec::new();
        for _ in 0..svc::MAX_PENDING {
            ids.push(w.new_intent().await.id);
        }
        assert!(matches!(
            svc::create(&w.pool, w.account, w.user, &w.tid, &w.aad, w.graph).await,
            Err(svc::CreateError::TooManyPending)
        ));
        // RLS: another member of the same account cannot read or stop it.
        let other = Uuid::new_v4();
        assert!(svc::get(&w.pool, w.account, other, ids[0]).await.unwrap().is_none());
        assert!(!svc::stop_by_user(&w.pool, w.account, other, ids[0]).await.unwrap());
        assert_eq!(w.reload(ids[0]).await.status, "pending_save");
        // Claim: bounded batch, wrong identity claims nothing, lease prevents double-claim.
        assert!(svc::claim_due(&w.pool, w.account, w.user, "other-tenant", &w.aad).await.unwrap().is_empty());
        let claimed = svc::claim_due(&w.pool, w.account, w.user, &w.tid, &w.aad).await.unwrap();
        assert_eq!(claimed.len() as i64, svc::CLAIM_BATCH);
        assert!(svc::claim_due(&w.pool, w.account, w.user, &w.tid, &w.aad).await.unwrap().is_empty());
        assert!(svc::next_delay(&w.pool, w.account, w.user).await.unwrap().is_some());
        // The rate cap is separate from the pending cap.
        for id in &ids {
            svc::stop_by_user(&w.pool, w.account, w.user, *id).await.unwrap();
        }
        for _ in 0..(svc::MAX_CREATED_PER_HOUR - svc::MAX_PENDING) {
            let c = svc::create(&w.pool, w.account, w.user, &w.tid, &w.aad, w.graph).await.ok().unwrap();
            svc::stop_by_user(&w.pool, w.account, w.user, c.id).await.unwrap();
        }
        assert!(matches!(
            svc::create(&w.pool, w.account, w.user, &w.tid, &w.aad, w.graph).await,
            Err(svc::CreateError::RateLimited)
        ));
        w.finish().await;
    }

    #[test]
    fn rotation_gives_every_member_a_turn_at_the_front() {
        let order: Vec<_> = (0..4).map(|t| rotate(vec!['a', 'b', 'c'], t)[0]).collect();
        assert_eq!(order, vec!['a', 'b', 'c', 'a']);
        assert!(rotate(Vec::<u8>::new(), 7).is_empty());
    }
}
