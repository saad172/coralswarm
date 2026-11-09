//! Which path a Library search was served by, and the `library_live_fallback`
//! event every Live serve logs. The event is the evidence that decides when
//! the deprecated Live path can go (stage 2).
use serde::Serialize;
use uuid::Uuid;
use super::{OutcomeVersions, SearchRequest};
use crate::tenancy::extractor::Tenant;

/// What `read_search_tx` served and why.
#[derive(Debug, PartialEq)]
pub(super) enum PathDecision {
    Model { freshness: Option<serde_json::Value> },
    /// `LIBRARY_READ_MODEL=off`, the kill switch.
    LiveKillSwitch,
    /// The raw `candidate_program` reason.
    LiveProgram { reason: String },
}
impl PathDecision {
    pub(super) fn into_freshness(self) -> Option<serde_json::Value> {
        match self { Self::Model { freshness } => freshness, _ => None }
    }
}

macro_rules! label_enum {
    ($name:ident { $($variant:ident),+ }) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
        #[serde(rename_all = "snake_case")]
        pub(super) enum $name { $($variant),+ }
    };
}
label_enum!(QueryLength { Empty, Short, Long });
label_enum!(CollectionShape { None, Meetings, Work, Other });
label_enum!(OutcomeMode { Retained, Current });
label_enum!(PageShape { First, Later });
// The bounded metric label for a Live serve: the program's reason folded
// into a fixed vocabulary, so the metric's cardinality stays fixed.
label_enum!(FallbackReason { Disabled, NotReady, ContextMismatch, AccessContractChanged, HistoryFloor, ScopedRequest, DirtyOverflow, Unsupported, Other });

pub(super) fn enum_text<T: Serialize>(value: T) -> String {
    serde_json::to_value(value).expect("enum serializes").as_str().expect("enum string").to_owned()
}

pub(super) fn fallback_reason(reason: &str) -> FallbackReason {
    match crate::read_access::Verdict::from_reason(reason) {
        Some(crate::read_access::Verdict::ContractChanged) => return FallbackReason::AccessContractChanged,
        Some(crate::read_access::Verdict::ClassMismatch) => return FallbackReason::ContextMismatch,
        Some(crate::read_access::Verdict::Admitted) | None => {}
    }
    match reason {
        "not_ready" => FallbackReason::NotReady,
        "entity_type_ambiguous" | "dirty_item_unresolved" | "listing_incomplete" => FallbackReason::Unsupported,
        "permission_window" => FallbackReason::HistoryFloor,
        "narrow_scope" => FallbackReason::ScopedRequest,
        "change_overflow" | "change_stale" | "raw_edge_overflow" | "dirty_hub" | "dirty_item_overflow" | "dirty_item_input_overflow" => FallbackReason::DirtyOverflow,
        _ => FallbackReason::Other,
    }
}

/// The request's shape, without its text, ids or cursors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ScopeShape {
    pub has_query: bool,
    pub query_length: QueryLength,
    pub origin_filtered: bool,
    pub collection: CollectionShape,
    pub outcome_mode: OutcomeMode,
    pub item_scoped: bool,
    pub version_scoped: bool,
    pub node_scoped: bool,
    pub page: PageShape,
}
pub(super) fn shape(q: &SearchRequest) -> ScopeShape {
    ScopeShape {
        has_query: !q.query.trim().is_empty(),
        query_length: match q.query.trim().chars().count() { 0 => QueryLength::Empty, 1..=3 => QueryLength::Short, _ => QueryLength::Long },
        origin_filtered: q.scope.origin_filter() != serde_json::json!({}),
        collection: match q.scope.collection_key() { None => CollectionShape::None, Some("meetings") => CollectionShape::Meetings, Some("work_sessions") => CollectionShape::Work, _ => CollectionShape::Other },
        outcome_mode: if q.scope.outcome_versions == Some(OutcomeVersions::Current) { OutcomeMode::Current } else { OutcomeMode::Retained },
        item_scoped: q.scope.source_item_id.is_some(),
        version_scoped: q.scope.source_version_id.is_some(),
        node_scoped: q.scope.node_id.is_some(),
        page: if q.sources_after.is_some() || q.outcomes_after.is_some() || q.nodes_after.is_some() { PageShape::Later } else { PageShape::First },
    }
}

#[derive(Debug)]
pub(super) struct LiveFallbackEvent {
    pub plane: &'static str,
    pub org_id: Option<String>,
    pub account_id: Uuid,
    pub graph_ids: Vec<Uuid>,
    pub shape: ScopeShape,
    pub mode: &'static str,
    /// "disabled", or the raw program reason.
    pub reason: String,
    /// The bounded metric label.
    pub reason_label: String,
    pub level: tracing::Level,
}
impl LiveFallbackEvent {
    pub(super) fn new(tenant: &Tenant, account: Uuid, q: &SearchRequest, decision: &PathDecision) -> Option<Self> {
        let (mode, reason, label, level) = match decision {
            PathDecision::Model { .. } => return None,
            PathDecision::LiveKillSwitch => ("off", "disabled", FallbackReason::Disabled, tracing::Level::INFO),
            PathDecision::LiveProgram { reason } => ("on", reason.as_str(), fallback_reason(reason), tracing::Level::WARN),
        };
        let org_id = tenant.org_context().map(|ctx| ctx.org_id.clone());
        Some(Self {
            plane: if org_id.is_some() { "org" } else { "consumer" },
            org_id,
            account_id: account,
            graph_ids: q.scope.graph_ids.clone(),
            shape: shape(q),
            mode,
            reason: reason.to_owned(),
            reason_label: enum_text(label),
            level,
        })
    }
}

/// One `library_live_fallback` event per Live-served search, carrying the
/// tenant ids on purpose (it is emitted under the request span), plus one
/// id-free metric line.
pub(super) fn log_live_fallback(event: &LiveFallbackEvent) {
    let shape = &event.shape;
    macro_rules! emit {($level:expr) => {tracing::event!($level, event = "library_live_fallback", plane = event.plane, org_id = event.org_id.as_deref(),
        account_id = %event.account_id, graph_ids = ?event.graph_ids, graphs = event.graph_ids.len(), has_query = shape.has_query,
        query_length = %enum_text(shape.query_length), origin_filtered = shape.origin_filtered, collection = %enum_text(shape.collection),
        outcome_mode = %enum_text(shape.outcome_mode), item_scoped = shape.item_scoped, version_scoped = shape.version_scoped,
        node_scoped = shape.node_scoped, page = %enum_text(shape.page), mode = event.mode, reason = %event.reason,
        "Library search served by the deprecated Live path")}}
    if event.level == tracing::Level::INFO { emit!(tracing::Level::INFO) } else { emit!(tracing::Level::WARN) }
    tracing::info!(target: crate::telemetry::METRICS_TARGET, metric = "library_live_fallback", plane = event.plane,
        reason = %event.reason_label, mode = event.mode, "Library search served by the deprecated Live path");
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::LibraryScope;
    use crate::tenancy::runtime::{TenancyDecision, TenantContext};
    use serde_json::{json, Value};
    use std::sync::{Arc, Mutex};

    const GRAPH: Uuid = Uuid::from_u128(0x6a);
    const ACCOUNT: Uuid = Uuid::from_u128(0xac);

    fn consumer() -> Tenant {
        Tenant {
            store: None,
            pool: None,
            decision: None,
            class: crate::route_class::RouteClass::Other,
        }
    }

    fn org() -> Tenant {
        let pool = sqlx::Pool::<sqlx::Postgres>::connect_lazy("postgres://127.0.0.1:1/none").unwrap();
        let ctx = TenantContext {
            org_id: "org_live".into(),
            account_id: ACCOUNT,
            user_id: ACCOUNT,
            pool,
            page_pool: None,
        };
        Tenant {
            store: None,
            pool: None,
            decision: Some(TenancyDecision::Org(ctx)),
            class: crate::route_class::RouteClass::Other,
        }
    }

    fn request() -> SearchRequest {
        SearchRequest {
            scope: LibraryScope {
                graph_ids: vec![GRAPH],
                outcome_versions: Some(OutcomeVersions::Current),
                service_id: None,
                connection: None,
                container: None,
                content_kind: None,
                collection: None,
                source_item_id: None,
                source_version_id: None,
                node_id: None,
            },
            query: "roadmap".into(),
            page_size: None,
            sources_after: None,
            outcomes_after: None,
            nodes_after: None,
        }
    }

    fn program(reason: &str) -> PathDecision {
        PathDecision::LiveProgram { reason: reason.into() }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn every_live_variant_names_its_cause_and_a_model_serve_has_no_event() {
        let q = request();
        let event = |tenant: &Tenant, decision: &PathDecision| LiveFallbackEvent::new(tenant, ACCOUNT, &q, decision)
            .map(|e| (e.plane, e.org_id, e.mode, e.reason, e.reason_label, e.level));
        let tenant = consumer();
        let info = tracing::Level::INFO;
        let warn = tracing::Level::WARN;
        assert_eq!(event(&tenant, &PathDecision::Model { freshness: Some(json!({"state": "updating"})) }), None);
        assert_eq!(event(&tenant, &PathDecision::LiveKillSwitch),
            Some(("consumer", None, "off", "disabled".into(), "disabled".into(), info)));
        assert_eq!(event(&tenant, &program("not_ready")),
            Some(("consumer", None, "on", "not_ready".into(), "not_ready".into(), warn)));
        assert_eq!(event(&tenant, &program("dirty_hub")),
            Some(("consumer", None, "on", "dirty_hub".into(), "dirty_overflow".into(), warn)));
        assert_eq!(event(&org(), &program("narrow_scope")),
            Some(("org", Some("org_live".into()), "on", "narrow_scope".into(), "scoped_request".into(), warn)));

        let full = LiveFallbackEvent::new(&tenant, ACCOUNT, &q, &program("not_ready")).unwrap();
        assert_eq!((full.account_id, full.graph_ids, full.shape), (ACCOUNT, vec![GRAPH], shape(&q)));
        assert_eq!(PathDecision::Model { freshness: Some(json!(1)) }.into_freshness(), Some(json!(1)));
        assert_eq!(program("not_ready").into_freshness(), None);
    }

    #[derive(Clone, Default)]
    struct LogBuf(Arc<Mutex<Vec<u8>>>);
    impl std::io::Write for LogBuf {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBuf {
        type Writer = Self;
        fn make_writer(&'a self) -> Self {
            self.clone()
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn log_live_fallback_emits_one_structured_line_and_one_bounded_metric_line() {
        let buf = LogBuf::default();
        let subscriber = tracing_subscriber::fmt().json().with_writer(buf.clone()).with_ansi(false).finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        let q = request();
        log_live_fallback(&LiveFallbackEvent::new(&org(), ACCOUNT, &q, &program("narrow_scope")).unwrap());
        log_live_fallback(&LiveFallbackEvent::new(&consumer(), ACCOUNT, &q, &PathDecision::LiveKillSwitch).unwrap());

        let logs = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        let lines: Vec<Value> = logs.lines().map(|l| serde_json::from_str(l).unwrap()).collect();
        let events: Vec<&Value> = lines.iter().filter(|v| v["fields"]["event"] == "library_live_fallback").collect();
        assert_eq!(events.len(), 2, "one event line per call: {logs}");
        let (org_line, consumer_line) = (&events[0], &events[1]);
        assert_eq!(org_line["level"], "WARN");
        assert_eq!(org_line["fields"]["plane"], "org");
        assert_eq!(org_line["fields"]["org_id"], "org_live");
        assert_eq!(org_line["fields"]["account_id"], ACCOUNT.to_string());
        assert!(org_line["fields"]["graph_ids"].as_str().unwrap().contains(&GRAPH.to_string()), "{org_line}");
        assert_eq!(org_line["fields"]["graphs"], 1);
        assert_eq!(org_line["fields"]["reason"], "narrow_scope");
        assert_eq!(org_line["fields"]["mode"], "on");
        assert_eq!(org_line["fields"]["query_length"], "long");
        assert_eq!(org_line["fields"]["node_scoped"], false);
        assert_eq!(consumer_line["level"], "INFO");
        assert_eq!(consumer_line["fields"]["plane"], "consumer");
        assert!(consumer_line["fields"].get("org_id").is_none(), "{consumer_line}");
        assert_eq!(consumer_line["fields"]["reason"], "disabled");

        let metrics: Vec<&Value> = lines.iter().filter(|v| v["target"] == crate::telemetry::METRICS_TARGET).collect();
        assert_eq!(metrics.len(), 2, "one metric line per call: {logs}");
        for metric in &metrics {
            let mut keys: Vec<&str> = metric["fields"].as_object().unwrap().keys().map(String::as_str).collect();
            keys.sort_unstable();
            assert_eq!(keys, ["message", "metric", "mode", "plane", "reason"], "no id in a metric line: {metric}");
            assert_eq!(metric["fields"]["metric"], "library_live_fallback");
        }
        assert_eq!((&metrics[0]["fields"]["reason"], &metrics[0]["fields"]["plane"]), (&json!("scoped_request"), &json!("org")));
        assert_eq!((&metrics[1]["fields"]["reason"], &metrics[1]["fields"]["mode"]), (&json!("disabled"), &json!("off")));
    }
}
