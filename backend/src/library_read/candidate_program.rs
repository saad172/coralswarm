//! One bounded candidate program, shared by serving and independent proofs.
//! The caller owns an explicitly read-only RR transaction and its rollback.
use std::{future::Future, time::Duration};

use serde::Deserialize;
use serde_json::Value;
use sqlx::{Postgres, Transaction};
use tokio::time::{timeout_at, Instant};

use super::{read_model, Cursor, LibraryScope};

#[derive(Debug, thiserror::Error)]
pub enum CandidateProgramError {
    #[error("Library candidate SQL failed")]
    Sql(#[from] sqlx::Error),
    #[error("Library candidate program deadline exceeded")]
    Deadline,
    #[error("Library candidate returned an invalid attempt")]
    InvalidAttempt,
}
impl CandidateProgramError {
    pub fn is_timeout(&self) -> bool {
        matches!(self, Self::Deadline)
            || matches!(self, Self::Sql(e) if e.as_database_error().and_then(|e| e.code()).is_some_and(|c| c == "57014"))
    }
}

pub(super) struct ResultValue {
    pub value: Value,
    pub path: &'static str,
    pub reason: Option<String>,
    /// Present only when a Model value was served with stale components
    /// (a hub, `read_model::HUB_STALE_SECS`): the public `freshness` object.
    pub freshness: Option<Value>,
    /// The oldest pending change the last attempt probed (RFC 3339), for the
    /// backlog alarm. Never part of the public value.
    pub backlog_oldest_at: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    value: Value,
    path: String,
    fallback_reason: Option<String>,
    #[serde(default)]
    freshness: Option<Value>,
    #[serde(default)]
    backlog_oldest_at: Option<String>,
}
enum Attempt {
    Model(Value, Option<Value>),
    NeedsDirty,
    NeedsLive(String),
}
/// The one stale shape a Model attempt may declare: nodes only, with the
/// start of the stale window.
fn valid_freshness(value: &Value) -> bool {
    value.as_object().is_some_and(|o| {
        o.len() == 3
            && o.get("state") == Some(&Value::from("updating"))
            && o.get("components") == Some(&serde_json::json!(["nodes"]))
            && o.get("stale_since").and_then(Value::as_str)
                .is_some_and(|t| chrono::DateTime::parse_from_rfc3339(t).is_ok())
    })
}
fn public_groups(value: &Value) -> bool {
    value.is_object()
        && ["sources", "outcomes", "nodes"]
            .iter()
            .all(|key| value[*key]["items"].is_array() && value[*key]["total"].as_u64().is_some())
}
fn decode(value: Value) -> Result<(Attempt, Option<String>), CandidateProgramError> {
    // Require all three keys, including an explicit JSON-null reason/value.
    if !["value", "path", "fallback_reason"]
        .iter()
        .all(|key| value.get(key).is_some())
    {
        return Err(CandidateProgramError::InvalidAttempt);
    }
    let envelope: Envelope =
        serde_json::from_value(value).map_err(|_| CandidateProgramError::InvalidAttempt)?;
    if envelope.backlog_oldest_at.as_deref().is_some_and(|t| chrono::DateTime::parse_from_rfc3339(t).is_err()) {
        return Err(CandidateProgramError::InvalidAttempt);
    }
    let backlog = envelope.backlog_oldest_at;
    // Only a Model value can be stale, and only in the declared shape.
    let freshness = match (envelope.path.as_str(), envelope.freshness) {
        (_, None) => None,
        ("model", Some(f)) if valid_freshness(&f) => Some(f),
        _ => return Err(CandidateProgramError::InvalidAttempt),
    };
    let attempt = match (envelope.path.as_str(), envelope.fallback_reason) {
        ("model", None) if public_groups(&envelope.value) => Ok(Attempt::Model(envelope.value, freshness)),
        ("dirty", None) if envelope.value.is_null() => Ok(Attempt::NeedsDirty),
        ("live", Some(reason))
            if envelope.value.is_null()
                && matches!(
                    reason.as_str(),
                    "narrow_scope"
                        | "permission_window"
                        | "not_ready"
                        | "context_mismatch"
                        | "access_contract_changed"
                        | "change_overflow"
                        | "change_stale"
                        | "raw_edge_overflow"
                        | "dirty_hub"
                        | "entity_type_ambiguous"
                        | "dirty_item_unresolved"
                        | "dirty_item_overflow"
                        | "dirty_item_input_overflow"
                        | "listing_incomplete"
                ) =>
        {
            Ok(Attempt::NeedsLive(reason))
        }
        _ => Err(CandidateProgramError::InvalidAttempt),
    }?;
    Ok((attempt, backlog))
}
fn remaining(deadline: Instant) -> Result<Duration, CandidateProgramError> {
    let left = deadline.saturating_duration_since(Instant::now());
    if left.is_zero() {
        Err(CandidateProgramError::Deadline)
    } else {
        Ok(left)
    }
}
async fn bounded<T>(
    deadline: Instant,
    future: impl Future<Output = Result<T, sqlx::Error>>,
) -> Result<T, CandidateProgramError> {
    remaining(deadline)?;
    let result = timeout_at(deadline, future)
        .await
        .map_err(|_| CandidateProgramError::Deadline)?;
    // Timeout polls the inner future first. A non-yielding completion can
    // finish after the deadline, so even a successful final restore must be
    // checked after its poll before the program can publish success.
    remaining(deadline)?;
    result.map_err(Into::into)
}

/// Cleanup has a separate bounded allowance and must be acknowledged before
/// a proof can publish a successful disposition. It never runs another reader.
pub(super) async fn cleanup(
    future: impl Future<Output = Result<(), sqlx::Error>>,
) -> Result<(), CandidateProgramError> {
    bounded(
        Instant::now() + Duration::from_millis(read_model::CLEANUP_WALL_MILLIS),
        future,
    )
    .await
}

/// Narrow I/O seam also lets clock-controlled tests exercise the real program
/// sequencing, rather than a second implementation of the deadline algorithm.
#[async_trait::async_trait]
trait Connection: Send {
    async fn original_timeout(&mut self) -> Result<i64, sqlx::Error>;
    async fn set_timeout(&mut self, millis: u64) -> Result<(), sqlx::Error>;
    async fn fetch(&mut self, sql: &str) -> Result<Value, sqlx::Error>;
}
pub(super) struct Arguments<'a> {
    pub scope: &'a LibraryScope,
    pub query: &'a str,
    pub limit: i64,
    pub sources: Option<&'a Cursor>,
    pub outcomes: Option<&'a Cursor>,
    pub nodes: Option<&'a Cursor>,
}
struct SqlConnection<'borrow, 'tx, 'args> {
    tx: &'borrow mut Transaction<'tx, Postgres>,
    args: Arguments<'args>,
}
#[async_trait::async_trait]
impl Connection for SqlConnection<'_, '_, '_> {
    async fn original_timeout(&mut self) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar("SELECT setting::bigint FROM pg_settings WHERE name='statement_timeout'")
            .fetch_one(&mut **self.tx)
            .await
    }
    async fn set_timeout(&mut self, millis: u64) -> Result<(), sqlx::Error> {
        sqlx::query("SELECT set_config('statement_timeout',$1,true)")
            .bind(format!("{millis}ms"))
            .execute(&mut **self.tx)
            .await?;
        Ok(())
    }
    async fn fetch(&mut self, sql: &str) -> Result<Value, sqlx::Error> {
        let args = &self.args;
        super::read_search_statement_tx(
            self.tx,
            sql,
            args.scope,
            args.query,
            args.limit,
            args.sources,
            args.outcomes,
            args.nodes,
        )
        .await
    }
}
async fn statement(
    connection: &mut impl Connection,
    sql: &str,
    deadline: Instant,
) -> Result<Value, CandidateProgramError> {
    let millis = remaining(deadline)?
        .as_millis()
        .max(1)
        .min(i32::MAX as u128) as u64;
    bounded(deadline, connection.set_timeout(millis)).await?;
    // The setting roundtrip and synchronous rendering consume the same wall.
    bounded(deadline, connection.fetch(sql)).await
}
pub(super) async fn execute(
    tx: &mut Transaction<'_, Postgres>,
    args: Arguments<'_>,
    budget: Duration,
) -> Result<ResultValue, CandidateProgramError> {
    let scope = args.scope;
    let query = args.query;
    run(
        tx,
        args,
        budget,
        || read_model::sql(scope, query),
        || super::search_sql_with_query(scope, query),
        || async {},
    )
    .await
}
/// Builders are lazy: Model never even renders or dispatches canonical Live.
/// Test-support callers may control SQL/interleavings through this same executor.
pub(super) async fn run<F: Future<Output = ()> + Send>(
    tx: &mut Transaction<'_, Postgres>,
    args: Arguments<'_>,
    budget: Duration,
    model: impl FnOnce() -> String + Send,
    live: impl FnOnce() -> String + Send,
    before_live: impl FnOnce() -> F + Send,
) -> Result<ResultValue, CandidateProgramError> {
    let scope=args.scope;
    let query=args.query;
    run_program_stages(
        &mut SqlConnection { tx, args },
        budget,
        model,
        || read_model::dirty_sql(scope,query),
        live,
        before_live,
    )
    .await
}
#[cfg(test)]
async fn run_program<F: Future<Output = ()> + Send>(
    connection: &mut impl Connection,budget:Duration,
    model:impl FnOnce()->String+Send,live:impl FnOnce()->String+Send,
    before_live:impl FnOnce()->F+Send,
)->Result<ResultValue,CandidateProgramError> {
    run_program_stages(connection,budget,model,||panic!("unexpected dirty dispatch"),live,before_live).await
}
async fn run_program_stages<F: Future<Output = ()> + Send>(
    connection: &mut impl Connection,
    budget: Duration,
    model: impl FnOnce() -> String + Send,
    dirty: impl FnOnce() -> String + Send,
    live: impl FnOnce() -> String + Send,
    before_live: impl FnOnce() -> F + Send,
) -> Result<ResultValue, CandidateProgramError> {
    let started = Instant::now();
    let mut deadline = started + budget;
    // Respect a caller's shorter SQL ceiling; zero means unlimited, which
    // never removes the program wall. No per-stage twelve-second reset.
    let original_ms = bounded(deadline, connection.original_timeout()).await?;
    if original_ms > 0 {
        deadline = deadline.min(started + Duration::from_millis(original_ms as u64));
    }
    let model_sql = model();
    remaining(deadline)?;
    let attempted = statement(connection, &model_sql, deadline).await?;
    let (attempt,backlog_oldest_at)=match decode(attempted)? {
        (Attempt::NeedsDirty,_)=>{
            let dirty_sql=dirty();
            remaining(deadline)?;
            match decode(statement(connection,&dirty_sql,deadline).await?)? {
                (Attempt::NeedsDirty,_)=>return Err(CandidateProgramError::InvalidAttempt),
                other=>other,
            }
        }
        other=>other,
    };
    let result = match attempt {
        Attempt::NeedsDirty=>return Err(CandidateProgramError::InvalidAttempt),
        Attempt::Model(value, freshness) => ResultValue {
            value,
            path: "model",
            reason: None,
            freshness,
            backlog_oldest_at,
        },
        Attempt::NeedsLive(reason) => {
            timeout_at(deadline, before_live())
                .await
                .map_err(|_| CandidateProgramError::Deadline)?;
            let live_sql = live();
            remaining(deadline)?;
            let value = statement(connection, &live_sql, deadline).await?;
            if !public_groups(&value) {
                return Err(CandidateProgramError::InvalidAttempt);
            }
            ResultValue {
                value,
                path: "live",
                reason: Some(reason),
                freshness: None,
                backlog_oldest_at,
            }
        }
    };
    // The original ceiling is restored INSIDE the same wall on success. Errors
    // never issue more SQL in a potentially aborted transaction; caller rolls back.
    bounded(deadline, connection.set_timeout(original_ms as u64)).await?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::VecDeque;

    fn groups() -> Value {
        json!({"sources":{"items":[],"total":0},"outcomes":{"items":[],"total":0},"nodes":{"items":[],"total":0}})
    }
    fn model() -> Value {
        json!({"value":groups(),"path":"model","fallback_reason":null})
    }
    fn needs_live() -> Value {
        json!({"value":null,"path":"live","fallback_reason":"not_ready"})
    }
    struct FakeConnection {
        // One delay for EACH I/O: original timeout, stage setting/fetch, restore.
        delays: VecDeque<u64>,
        fetched: VecDeque<Result<Value, sqlx::Error>>,
        events: Vec<String>,
        original: i64,
        nonyielding_restore_ms: u64,
    }
    impl FakeConnection {
        fn new(delays: &[u64], fetched: Vec<Result<Value, sqlx::Error>>) -> Self {
            Self {
                delays: delays.iter().copied().collect(),
                fetched: fetched.into(),
                events: vec![],
                original: 0,
                nonyielding_restore_ms: 0,
            }
        }
        async fn pause(&mut self) {
            tokio::time::sleep(Duration::from_millis(
                self.delays.pop_front().expect("one delay per actual I/O"),
            ))
            .await;
        }
    }
    #[async_trait::async_trait]
    impl Connection for FakeConnection {
        async fn original_timeout(&mut self) -> Result<i64, sqlx::Error> {
            self.events.push("original".into());
            self.pause().await;
            Ok(self.original)
        }
        async fn set_timeout(&mut self, millis: u64) -> Result<(), sqlx::Error> {
            self.events.push(format!("set:{millis}"));
            self.pause().await;
            if millis == self.original as u64 && self.nonyielding_restore_ms > 0 {
                std::thread::sleep(Duration::from_millis(self.nonyielding_restore_ms));
            }
            Ok(())
        }
        async fn fetch(&mut self, sql: &str) -> Result<Value, sqlx::Error> {
            self.events.push(sql.into());
            self.pause().await;
            self.fetched
                .pop_front()
                .expect("one response per actual dispatch")
        }
    }
    #[test]
    fn attempt_protocol_rejects_missing_unknown_and_inconsistent_values() {
        for bad in [
            json!({"path":"live","value":null}),
            json!({"path":"live","value":null,"fallback_reason":"unknown"}),
            json!({"path":"model","value":null,"fallback_reason":null}),
            json!({"path":"live","value":{},"fallback_reason":"not_ready"}),
            json!({"path":"live","value":null,"fallback_reason":"not_ready","extra":true}),
        ] {
            assert!(decode(bad).is_err());
        }
        assert!(matches!(decode(needs_live()), Ok((Attempt::NeedsLive(_), None))));
        for removed in ["dirty_node_overflow", "dirty_input_overflow"] {
            assert!(decode(json!({"path":"live","value":null,"fallback_reason":removed})).is_err(), "{removed} is no longer emitted");
        }
        assert!(matches!(decode(json!({"path":"live","value":null,"fallback_reason":"dirty_hub"})), Ok((Attempt::NeedsLive(r), None)) if r == "dirty_hub"));
    }
    #[test]
    fn freshness_is_model_only_and_strictly_shaped() {
        let stale = json!({"state":"updating","stale_since":"2026-10-28T12:00:00.5+00:00","components":["nodes"]});
        let at = "2026-10-28T11:59:00+00:00";
        match decode(json!({"value":groups(),"path":"model","fallback_reason":null,"freshness":stale,"backlog_oldest_at":at})) {
            Ok((Attempt::Model(_, Some(f)), Some(b))) => { assert_eq!(f, stale); assert_eq!(b, at); }
            _ => panic!("a stale Model value decodes with its marker"),
        }
        assert!(matches!(decode(json!({"value":groups(),"path":"model","fallback_reason":null,"freshness":null,"backlog_oldest_at":null})), Ok((Attempt::Model(_, None), None))));
        for bad in [
            json!({"value":null,"path":"live","fallback_reason":"dirty_hub","freshness":stale}),
            json!({"value":null,"path":"dirty","fallback_reason":null,"freshness":stale}),
            json!({"value":groups(),"path":"model","fallback_reason":null,"freshness":{"state":"updating","stale_since":"2026-10-28T12:00:00Z","components":["sources"]}}),
            json!({"value":groups(),"path":"model","fallback_reason":null,"freshness":{"state":"current"}}),
            json!({"value":groups(),"path":"model","fallback_reason":null,"freshness":{"state":"updating","stale_since":"yesterday","components":["nodes"]}}),
            json!({"value":groups(),"path":"model","fallback_reason":null,"backlog_oldest_at":"soon"}),
        ] { assert!(decode(bad).is_err()); }
    }
    #[tokio::test(start_paused = true)]
    async fn cleanup_requires_successful_ack_within_its_separate_wall() {
        assert!(cleanup(async { Ok(()) }).await.is_ok());
        assert!(matches!(
            cleanup(async { Err(sqlx::Error::Protocol("rollback failed".into())) }).await,
            Err(CandidateProgramError::Sql(_))
        ));
        let start = Instant::now();
        let result = cleanup(async {
            tokio::time::sleep(Duration::from_millis(read_model::CLEANUP_WALL_MILLIS + 1)).await;
            Ok(())
        })
        .await;
        assert!(matches!(result, Err(CandidateProgramError::Deadline)));
        assert_eq!(
            start.elapsed(),
            Duration::from_millis(read_model::CLEANUP_WALL_MILLIS)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn model_never_renders_live_and_restores_original_ceiling() {
        let mut io = FakeConnection::new(&[1, 2, 3, 4], vec![Ok(model())]);
        io.original = 50;
        let result = run_program(
            &mut io,
            Duration::from_secs(12),
            || "model".into(),
            || panic!("Live must stay lazy"),
            || async { panic!("not a fallback") },
        )
        .await
        .unwrap();
        assert_eq!(result.path, "model");
        assert_eq!(io.events, ["original", "set:49", "model", "set:50"]);
    }
    #[tokio::test(start_paused = true)]
    async fn only_successful_known_needs_live_can_render_live() {
        for response in [
            Err(sqlx::Error::Protocol("fixture SQL error".into())),
            Ok(json!({"value":null,"path":"live","fallback_reason":"unknown"})),
            Ok(json!({"value":null,"path":"model","fallback_reason":null})),
        ] {
            let mut io = FakeConnection::new(&[1, 1, 1], vec![response]);
            assert!(run_program(
                &mut io,
                Duration::from_millis(100),
                || "model".into(),
                || panic!("failure cannot render Live"),
                || async { panic!("failure cannot enter fallback") }
            )
            .await
            .is_err());
            assert_eq!(io.events.len(), 3, "no restore/retry SQL after failure");
        }
    }
    #[tokio::test(start_paused = true)]
    async fn fallback_fetch_and_both_settings_share_one_deadline() {
        // Read ceiling + Model setting/fetch + Live setting consume 70/100ms.
        // Live cannot receive a fresh 100ms: its fetch crosses the remaining30.
        let mut io =
            FakeConnection::new(&[10, 20, 20, 20, 40], vec![Ok(needs_live()), Ok(groups())]);
        let start = Instant::now();
        let result = run_program(
            &mut io,
            Duration::from_millis(100),
            || "model".into(),
            || "live".into(),
            || async {},
        )
        .await;
        assert!(matches!(result, Err(CandidateProgramError::Deadline)));
        assert_eq!(start.elapsed(), Duration::from_millis(100));
        assert_eq!(io.events, ["original", "set:90", "model", "set:50", "live"]);
        // Each configuration roundtrip is itself bounded, before any fetch.
        for delays in [&[0, 101][..], &[0, 0, 0, 101][..]] {
            let mut io = FakeConnection::new(delays, vec![Ok(needs_live())]);
            assert!(matches!(
                run_program(
                    &mut io,
                    Duration::from_millis(100),
                    || "model".into(),
                    || "live".into(),
                    || async {}
                )
                .await,
                Err(CandidateProgramError::Deadline)
            ));
            assert!(!io.events.iter().any(|e| e == "live"));
        }
    }
    #[tokio::test(start_paused = true)]
    async fn timeout_read_and_final_restore_are_inside_wall() {
        for delays in [&[101][..], &[0, 0, 0, 101][..]] {
            let mut io = FakeConnection::new(delays, vec![Ok(model())]);
            assert!(matches!(
                run_program(
                    &mut io,
                    Duration::from_millis(100),
                    || "model".into(),
                    || panic!("no fallback"),
                    || async {}
                )
                .await,
                Err(CandidateProgramError::Deadline)
            ));
        }
    }
    #[tokio::test]
    async fn late_nonyielding_restore_cannot_publish_success() {
        let mut io = FakeConnection::new(&[0, 0, 0, 0], vec![Ok(model())]);
        io.nonyielding_restore_ms = 150;
        let result = run_program(
            &mut io,
            Duration::from_millis(100),
            || "model".into(),
            || panic!("no fallback"),
            || async {},
        )
        .await;
        assert!(matches!(result, Err(CandidateProgramError::Deadline)));
        assert_eq!(
            io.events.last().map(String::as_str),
            Some("set:0"),
            "the final I/O completed successfully but beyond the program wall"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn elapsed_pre_render_and_fallback_render_prevent_dispatch() {
        // Synchronous rendering cannot be preempted; the immediate remaining
        // check must reject before dispatch. A clock advance models that CPU.
        let mut io = FakeConnection::new(&[0, 0, 0], vec![Ok(needs_live())]);
        let result = run_program(
            &mut io,
            Duration::from_millis(100),
            || "model".into(),
            || panic!("expired before Live render"),
            || async { tokio::time::sleep(Duration::from_millis(101)).await },
        )
        .await;
        assert!(matches!(result, Err(CandidateProgramError::Deadline)));
        assert_eq!(io.events.len(), 3);
    }
    #[tokio::test]
    async fn synchronous_rendering_is_charged_before_each_stage_dispatch() {
        for fallback in [false, true] {
            let mut io = FakeConnection::new(
                if fallback { &[0, 0, 0] } else { &[0] },
                vec![Ok(needs_live())],
            );
            let render = || {
                std::thread::sleep(Duration::from_millis(25));
                "expired".into()
            };
            let result = if fallback {
                run_program(
                    &mut io,
                    Duration::from_millis(15),
                    || "model".into(),
                    render,
                    || async {},
                )
                .await
            } else {
                run_program(
                    &mut io,
                    Duration::from_millis(15),
                    render,
                    || panic!("not reached"),
                    || async {},
                )
                .await
            };
            assert!(matches!(result, Err(CandidateProgramError::Deadline)));
            assert_eq!(io.events.len(), if fallback { 3 } else { 1 });
        }
    }
    fn needs_dirty() -> Value {
        json!({"value":null,"path":"dirty","fallback_reason":null})
    }
    #[test]
    fn dirty_envelope_is_internal_and_strict() {
        assert!(matches!(decode(needs_dirty()),Ok((Attempt::NeedsDirty,None))));
        for bad in [
            json!({"value":groups(),"path":"dirty","fallback_reason":null}),
            json!({"value":null,"path":"dirty","fallback_reason":"not_ready"}),
            json!({"value":null,"path":"dirty"}),
            json!({"value":null,"path":"dirty","fallback_reason":null,"extra":false}),
        ] { assert!(decode(bad).is_err()); }
    }
    #[tokio::test(start_paused = true)]
    async fn clean_to_dirty_uses_same_connection_and_remaining_ceiling() {
        let mut io=FakeConnection::new(&[1,2,3,4,5,6],vec![Ok(needs_dirty()),Ok(model())]);
        io.original=100;
        let result=run_program_stages(&mut io,Duration::from_secs(12),
            ||"clean".into(),||"dirty".into(),||panic!("Live stays lazy"),||async{panic!("no fallback")}).await.unwrap();
        assert_eq!(result.path,"model");
        assert_eq!(io.events,["original","set:99","clean","set:94","dirty","set:100"]);
        assert!(io.fetched.is_empty());
    }
    #[tokio::test(start_paused = true)]
    async fn dirty_failure_or_second_needs_dirty_never_dispatches_live_or_restore() {
        for reply in [Err(sqlx::Error::Protocol("fixture".into())),Ok(needs_dirty()),
            Ok(json!({"value":null,"path":"unexpected","fallback_reason":null}))] {
            let mut io=FakeConnection::new(&[0,0,0,0,0],vec![Ok(needs_dirty()),reply]);
            assert!(run_program_stages(&mut io,Duration::from_millis(100),
                ||"clean".into(),||"dirty".into(),||panic!("no failure fallback"),||async{panic!("not reached")}).await.is_err());
            assert_eq!(io.events.len(),5);
        }
    }
    #[tokio::test(start_paused = true)]
    async fn three_stage_success_preserves_live_reason_and_single_restore() {
        let mut io=FakeConnection::new(&[1,1,1,1,1,1,1,1],vec![Ok(needs_dirty()),Ok(needs_live()),Ok(groups())]);
        let result=run_program_stages(&mut io,Duration::from_millis(100),
            ||"clean".into(),||"dirty".into(),||"live".into(),||async{}).await.unwrap();
        assert_eq!(result.path,"live");assert_eq!(result.reason.as_deref(),Some("not_ready"));
        assert_eq!(io.events,["original","set:99","clean","set:97","dirty","set:95","live","set:0"]);
    }
    #[tokio::test(start_paused = true)]
    async fn dirty_and_live_cannot_reset_the_program_wall() {
        let mut io=FakeConnection::new(&[10,10,25,10,25,10,20],vec![Ok(needs_dirty()),Ok(needs_live()),Ok(groups())]);
        let start=Instant::now();
        let result=run_program_stages(&mut io,Duration::from_millis(100),
            ||"clean".into(),||"dirty".into(),||"live".into(),||async{}).await;
        assert!(matches!(result,Err(CandidateProgramError::Deadline)));
        assert_eq!(start.elapsed(),Duration::from_millis(100));
        assert_eq!(io.events,["original","set:90","clean","set:55","dirty","set:20","live"]);
    }

}
