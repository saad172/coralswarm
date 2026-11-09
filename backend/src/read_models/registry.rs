use super::*;
use axum::Router;
use serde_json::Value;
use std::{marker::PhantomData, sync::Arc, time::Duration};

pub trait ReadModelDyn: Send + Sync {
    fn name(&self) -> &'static str;
    fn path(&self) -> &'static str;
    fn version(&self) -> i32;
    fn debounce(&self) -> Duration;
    fn max_delay(&self) -> Duration;
    fn compute_timeout(&self) -> Duration;
    fn control_facts(&self) -> bool;
    fn access(&self) -> access::AccessPolicy;
    fn account_facts(&self) -> bool;
    fn dirty_triggers(&self) -> &'static [dirty::DirtyTrigger];
    fn compute_json<'a>(&self, cx: ComputeCx<'a>, params: Value) -> ComputeFuture<'a, Value>;
    fn verify_scope(
        &self,
        viewer: &Viewer,
        params: Value,
        key: &str,
        graphs: &[Uuid],
    ) -> Result<(), Error>;
    fn routes(&self) -> Router<Arc<crate::AppState>>;
}
struct Model<M>(PhantomData<M>);
impl<M: ReadModel> ReadModelDyn for Model<M> {
    fn name(&self) -> &'static str {
        M::NAME
    }
    fn path(&self) -> &'static str {
        M::PATH
    }
    fn version(&self) -> i32 {
        M::VERSION
    }
    fn debounce(&self) -> Duration {
        M::DEBOUNCE
    }
    fn max_delay(&self) -> Duration {
        M::MAX_DELAY
    }
    fn compute_timeout(&self) -> Duration {
        M::COMPUTE_TIMEOUT
    }
    fn control_facts(&self) -> bool {
        M::CONTROL_FACTS
    }
    fn account_facts(&self) -> bool {
        M::ACCOUNT_FACTS
    }
    fn access(&self) -> access::AccessPolicy {
        M::ACCESS
    }
    fn dirty_triggers(&self) -> &'static [dirty::DirtyTrigger] {
        M::dirty_triggers()
    }
    fn compute_json<'a>(&self, cx: ComputeCx<'a>, params: Value) -> ComputeFuture<'a, Value> {
        Box::pin(async move {
            let p = serde_json::from_value(params)?;
            Ok(serde_json::to_value(M::compute(cx, &p).await?)?)
        })
    }
    fn verify_scope(
        &self,
        viewer: &Viewer,
        params: Value,
        key: &str,
        graphs: &[Uuid],
    ) -> Result<(), Error> {
        // `Viewer::scope` refuses a named graph that left the actor's visible
        // set as `Unaddressable::ScopeGraphs`; its other errors (an
        // uncertified viewer, a serialization failure) keep their own kind.
        let scope = viewer.scope::<M>(serde_json::from_value(params)?)?;
        if scope.key() != key || scope.class.graphs() != graphs {
            return Err(Box::new(Unaddressable::ViewerClass));
        }
        Ok(())
    }
    fn routes(&self) -> Router<Arc<crate::AppState>> {
        crate::routes::page_read::get::<M>(M::PATH)
            .merge(crate::routes::page_read::refresh::<M>(M::PATH))
    }
}
macro_rules! read_models {
 ($($model:ty),+ $(,)?)=>{pub fn all()->&'static [&'static dyn ReadModelDyn]{&[$(&Model::<$model>(PhantomData)),+]}};
}
// The only registration list: routes, refresh dispatch and manifest derive here.
read_models! {
    oceans::WorkspaceCounts,
    home::HomeMetrics, home::WorkspaceActivity,
    today::JournalActivities, today::JournalStrip, today::JournalGist, today::JournalRollups,
    connectors::ConnectorsList, connectors::ConnectorDetail,
    connectors::ConnectorStatus, connectors::TeamsTranscriptCaptures,
    chats::ConversationList,
    oceans_pages::ReefPage, oceans_pages::OceansIndex, oceans_pages::Atlas,
    topics::TopicPage, chats_detail::ConversationDetail, chats_detail::ChatEmpty,
    org::MembersAdmin, library::CoralDetail, library::CaptureDetail, billing::UsageSummary,
}
pub fn by_name(name: &str) -> Option<&'static dyn ReadModelDyn> {
    all().iter().copied().find(|m| m.name() == name)
}
pub fn router() -> Router<Arc<crate::AppState>> {
    all().iter().fold(Router::new(), |r, m| r.merge(m.routes()))
}
pub fn manifest() -> Value {
    Value::Object(
        all()
            .iter()
            .map(|m| (m.name().into(), model_manifest(*m)))
            .collect(),
    )
}
fn model_manifest(m: &dyn ReadModelDyn) -> Value {
    use crate::workers::read_model_refresh as worker;
    serde_json::json!({"path":m.path(),"version":m.version(),"debounce_seconds":m.debounce().as_secs(),"max_delay_seconds":m.max_delay().as_secs(),
        "compute_timeout_ms":m.compute_timeout().as_millis(),
        "compute_total_timeout_ms":worker::compute_total_timeout(m).as_millis(),
        "worker_base_poll_ms":worker::BASE_POLL.as_millis()})
}
pub async fn sync_triggers(tx: &mut PgConnection) -> Result<(), Error> {
    for m in all() {
        // Additive union during rolling deployment. An older binary must
        // not remove a newer version's extra table subscription for the SAME
        // model name. Retired metadata can be collected with retired rows.
        for trigger in m.dirty_triggers() {
            if let dirty::DirtyTrigger::Table(table) = trigger {
                sqlx::query("INSERT INTO read_model_triggers(table_name,model) VALUES($1,$2) ON CONFLICT DO NOTHING").bind(table).bind(m.name()).execute(&mut *tx).await?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn manifest_timing_uses_runtime_limits_and_trait_overrides() {
        use crate::workers::read_model_refresh as worker;
        for m in all() {
            let entry = &manifest()[m.name()];
            assert_eq!(
                entry["compute_timeout_ms"],
                m.compute_timeout().as_millis() as u64
            );
            assert_eq!(
                entry["compute_total_timeout_ms"],
                worker::compute_total_timeout(*m).as_millis() as u64
            );
            assert_eq!(
                entry["worker_base_poll_ms"],
                worker::BASE_POLL.as_millis() as u64
            );
        }
        // Metadata-only fixture is never registered or dispatched. A nondefault
        // fractional-second trait budget must survive dynamic dispatch.
        struct Override;
        impl ReadModel for Override {
            const NAME: &'static str = "metadata_timeout_probe";
            const PATH: &'static str = "/metadata-timeout-probe";
            const VERSION: i32 = 1;
            const COMPUTE_TIMEOUT: Duration = Duration::from_millis(1250);
            type Params = NoParams;
            type Payload = Value;
            fn compute<'a>(_: ComputeCx<'a>, _: &'a NoParams) -> ComputeFuture<'a, Value> {
                Box::pin(async { panic!("metadata-only fixture must not compute") })
            }
            fn empty(_: &NoParams) -> Value {
                Value::Null
            }
            fn dirty_triggers() -> &'static [dirty::DirtyTrigger] { &[] }
        }
        let overridden = Model::<Override>(PhantomData);
        let entry = model_manifest(&overridden);
        assert_eq!(entry["compute_timeout_ms"], 1250);
        assert_eq!(entry["compute_total_timeout_ms"], 6250);
        assert_eq!(
            worker::compute_total_timeout(&overridden),
            Override::COMPUTE_TIMEOUT + worker::COMPUTE_GRACE
        );
        assert_eq!(
            entry["worker_base_poll_ms"],
            worker::BASE_POLL.as_millis() as u64
        );
    }
    /// Only a genuine refusal is `scope_unaddressable`; an uncertified viewer
    /// or unreadable parameters keep their own kind.
    #[test]
    fn verify_scope_names_only_genuine_refusals_unaddressable() {
        use super::super::failure::{classify, FailureKind as K};
        use oceans::{WorkspaceCounts, WorkspaceCountsParams as P};
        use Unaddressable as U;
        let (account, user, visible, gone) = (
            Uuid::from_u128(1),
            Uuid::from_u128(2),
            Uuid::from_u128(3),
            Uuid::from_u128(4),
        );
        let viewer = Viewer {
            account_id: account,
            user_id: user,
            windows: [(visible, None)].into(),
            control_revision: None,
            control_freshness: 0,
        };
        let counts = by_name("workspace_counts").unwrap();
        let params = |graph| serde_json::to_value(P { graph }).unwrap();
        let verify = |m: &dyn ReadModelDyn, params, key: &str, graphs: &[Uuid]| {
            classify(&m.verify_scope(&viewer, params, key, graphs).unwrap_err())
        };
        let scope = viewer.scope::<WorkspaceCounts>(P { graph: visible }).unwrap();
        assert!(counts
            .verify_scope(&viewer, params(visible), scope.key(), &[visible])
            .is_ok());
        let f = verify(counts, params(gone), scope.key(), &[gone]);
        assert_eq!((f.kind, f.reason()), (K::ScopeUnaddressable, Some(U::ScopeGraphs)));
        let f = verify(counts, params(visible), "stale", &[visible]);
        assert_eq!((f.kind, f.reason()), (K::ScopeUnaddressable, Some(U::ViewerClass)));
        let f = verify(counts, serde_json::json!({ "graph": 7 }), scope.key(), &[visible]);
        assert_eq!((f.kind, f.reason()), (K::Json, None));
        // An invariant failure: the worker certifies control facts first.
        let chat = by_name("chat_empty").unwrap();
        assert!(chat.control_facts());
        let p = home::WorkspaceActivityParams {
            limit: 5,
            since_days: 7,
            calendar_day: chrono::NaiveDate::from_ymd_opt(2026, 11, 3).unwrap(),
        };
        let f = verify(chat, serde_json::to_value(p).unwrap(), scope.key(), &[]);
        assert_eq!((f.kind, f.reason()), (K::Other, None));
    }
    #[test]
    fn registry_names_paths_and_manifest_are_unique() {
        let mut names = std::collections::HashSet::new();
        let mut paths = std::collections::HashSet::new();
        for m in all() {
            assert!(
                !m.name().is_empty() && m.name() != "*",
                "reserved actor-fence model name"
            );
            assert!(names.insert(m.name()));
            assert!(paths.insert(m.path()));
            assert!(by_name(m.name()).is_some());
            assert_eq!(manifest()[m.name()]["version"], m.version());
        }
        for required in [
            "workspace_counts",
            "home_metrics",
            "workspace_activity",
            "journal_activities",
            "journal_strip",
            "journal_gist",
            "journal_rollups",
            "connectors_list",
            "connector_detail",
            "connector_status",
            "teams_transcript_captures",
            "conversation_list",
        ] {
            assert!(
                names.contains(required),
                "approved L3 model missing: {required}"
            );
        }
    }
}
