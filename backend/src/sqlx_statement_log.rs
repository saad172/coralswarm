//! Test-only: record every SQL statement sqlx sends while a future runs.
//!
//! sqlx emits one `tracing` event (target `sqlx::query`, DEBUG) per executed
//! statement, carrying the statement text. Scoping a subscriber to ONE future
//! with [`record`] therefore yields the exact list of statements that future
//! caused — the round-trip count a request pays, measured in-process, with no
//! Postgres log to parse.
//!
//! Two things to know when reading a count:
//!
//! * `BEGIN` is NOT in the list. sqlx issues it through
//!   `queue_simple_query` (no logger), while `COMMIT`/`ROLLBACK` go through
//!   `execute` and ARE listed. Add one per transaction opened.
//! * Work on a `tokio::spawn`ed task is NOT in the list — the subscriber is
//!   scoped to the future it wraps, and a spawned task is polled outside it.
//!   This is the property that makes the numbers meaningful: the
//!   fire-and-forget access-log and display-name writes `require_auth`
//!   spawns are off the request's critical path by design, and they stay out
//!   of its count too.
//!
//! Compiled only under `cfg(any(test, feature = "test-support"))` — see
//! `lib.rs`'s `test_app_state` for why that gate (and not plain
//! `#[cfg(test)]`) is what lets `backend/tests/*.rs` reach it.

use std::future::Future;
use std::sync::{Arc, Mutex, OnceLock};

use tracing::field::{Field, Visit};
use tracing::instrument::WithSubscriber;
use tracing::Subscriber;
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};

/// The statements a [`record`] call observed, in execution order.
#[derive(Clone, Default, Debug)]
pub struct StatementLog(Arc<Mutex<Vec<String>>>);

impl StatementLog {
    /// Every statement text, in order.
    pub fn statements(&self) -> Vec<String> {
        self.0.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }

    /// How many statements ran.
    pub fn count(&self) -> usize {
        self.0.lock().unwrap_or_else(|p| p.into_inner()).len()
    }

    /// How many statements contain `needle` (a table or column name is the
    /// usual probe: `"clerk_org_id"` picks out every control-plane identity
    /// read on the org path).
    pub fn count_containing(&self, needle: &str) -> usize {
        self.statements().iter().filter(|s| s.contains(needle)).count()
    }

    fn push(&self, statement: String) {
        self.0
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(statement);
    }
}

struct StatementLayer(StatementLog);

impl<S: Subscriber> Layer<S> for StatementLayer {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        if event.metadata().target() != "sqlx::query" {
            return;
        }
        let mut visitor = StatementVisitor::default();
        event.record(&mut visitor);
        // `db.statement` carries the full SQL only when sqlx truncated the
        // summary; otherwise the summary IS the whole statement.
        let text = visitor
            .statement
            .filter(|s| !s.trim().is_empty())
            .or(visitor.summary)
            .unwrap_or_default();
        self.0.push(text.split_whitespace().collect::<Vec<_>>().join(" "));
    }
}

#[derive(Default)]
struct StatementVisitor {
    summary: Option<String>,
    statement: Option<String>,
}

impl Visit for StatementVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "summary" => self.summary = Some(value.to_string()),
            "db.statement" => self.statement = Some(value.to_string()),
            _ => {}
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        let rendered = format!("{value:?}");
        let rendered = rendered.trim_matches('"').to_string();
        match field.name() {
            "summary" => self.summary = Some(rendered),
            "db.statement" => self.statement = Some(rendered),
            _ => {}
        }
    }
}

/// Run `fut` with a statement-recording subscriber scoped to it, returning
/// its output alongside the statements it caused.
pub async fn record<F: Future>(fut: F) -> (F::Output, StatementLog) {
    // tracing-core's single-dispatcher fast path registers new callsites using
    // the registering thread's default subscriber. A SQLx enabled! callsite
    // first hit on an unrelated thread can therefore cache `never` while this
    // recorder is alive, and stay disabled when our future next polls it.
    // Keep a second, inert dispatcher registered before creating the recorder
    // so interest is computed across dispatchers instead. It is never installed
    // as a default and cannot collect events. Keeping it alive also avoids
    // concurrent recorders briefly returning to the single-dispatcher path.
    static INERT_DISPATCH: OnceLock<tracing::Dispatch> = OnceLock::new();
    INERT_DISPATCH.get_or_init(|| {
        tracing::Dispatch::new(tracing::subscriber::NoSubscriber::new())
    });
    let log = StatementLog::default();
    let subscriber = tracing_subscriber::registry().with(StatementLayer(log.clone()));
    // Keep the completed inner future's Drop inside the dispatcher scope too.
    #[allow(clippy::redundant_async_block)]
    let out = async move { fut.await }.with_subscriber(subscriber).await;
    (out, log)
}

#[cfg(test)]
mod tests {
    use super::record;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::{atomic::{AtomicUsize, Ordering}, Arc};
    use std::task::{Context, Poll};

    // Share both static callsites between the off-thread first hit and the
    // recorded query, matching SQLx's enabled!-then-event logging shape.
    fn sqlx_style_query(statement: &str) -> bool {
        let enabled = tracing::enabled!(target: "sqlx::query", tracing::Level::DEBUG);
        if enabled {
            tracing::debug!(target: "sqlx::query", summary = statement);
        }
        enabled
    }

    #[test]
    fn off_thread_first_hit_does_not_disable_owned_queries() {
        // A fresh process prevents another test's dispatcher or previously
        // registered callsite from masking the sole-recorder failure.
        const CHILD: &str = "CORALSWARM_STATEMENT_FIRST_HIT_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let result = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "sqlx_statement_log::tests::off_thread_first_hit_does_not_disable_owned_queries",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env(CHILD, "1")
                .output()
                .expect("isolated recorder regression must run");
            assert!(result.status.success(), "isolated regression failed: {}{}",
                String::from_utf8_lossy(&result.stdout),
                String::from_utf8_lossy(&result.stderr));
            assert!(String::from_utf8_lossy(&result.stdout).contains("1 passed; 0 failed"),
                "isolated regression must execute exactly one test: {}",
                String::from_utf8_lossy(&result.stdout));
            return;
        }

        let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
        runtime.block_on(async {
            let (enabled, first_log) = record(async {
                let off_thread = std::thread::spawn(|| {
                    tracing::dispatcher::with_default(&tracing::Dispatch::none(), || {
                        sqlx_style_query("SELECT unowned_first_hit")
                    })
                }).join().expect("first-hit thread must retire");
                assert!(!off_thread);
                sqlx_style_query("SELECT owned_first")
            }).await;
            assert!(enabled, "off-thread registration must not disable our query");
            assert_eq!(first_log.statements(), vec!["SELECT owned_first"]);

            assert!(!sqlx_style_query("SELECT unowned_after_scope"));
            let (_, second_log) = record(async {
                assert!(sqlx_style_query("SELECT owned_second"));
            }).await;
            assert_eq!(second_log.statements(), vec!["SELECT owned_second"]);
            assert_eq!(first_log.statements(), vec!["SELECT owned_first"]);
        });
    }

    #[tokio::test]
    async fn interleaved_recorders_keep_queries_in_their_own_scope() {
        let (first, second) = tokio::join!(
            record(async {
                sqlx_style_query("SELECT first_before_yield");
                tokio::task::yield_now().await;
                tokio::spawn(async {
                    sqlx_style_query("SELECT spawned_unowned");
                }).await.unwrap();
                sqlx_style_query("SELECT first_after_yield");
            }),
            record(async {
                sqlx_style_query("SELECT second_before_yield");
                tokio::task::yield_now().await;
                sqlx_style_query("SELECT second_after_yield");
            }),
        );
        assert_eq!(first.1.statements(), vec![
            "SELECT first_before_yield", "SELECT first_after_yield",
        ]);
        assert_eq!(second.1.statements(), vec![
            "SELECT second_before_yield", "SELECT second_after_yield",
        ]);
    }

    struct StatementOnDrop {
        polls: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
    }

    impl Future for StatementOnDrop {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(())
        }
    }

    impl Drop for StatementOnDrop {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
            tracing::debug!(target: "sqlx::query", summary = "SELECT drop_probe");
        }
    }

    #[tokio::test]
    async fn completed_future_drop_keeps_statement_subscriber() {
        let polls = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let (_, log) = record(StatementOnDrop {
            polls: polls.clone(),
            drops: drops.clone(),
        }).await;
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(log.statements(), vec!["SELECT drop_probe"]);
    }

    #[tokio::test]
    async fn polling_preserves_output_and_records_only_query_events() {
        let (output, log) = record(async {
            tracing::debug!(target: "unrelated", summary = "SELECT excluded_probe");
            tracing::debug!(target: "sqlx::query", summary = "SELECT poll_probe");
            73
        }).await;
        assert_eq!(output, 73);
        assert_eq!(log.statements(), vec!["SELECT poll_probe"]);
    }
}
