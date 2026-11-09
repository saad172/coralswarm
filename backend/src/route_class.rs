//! Which budget a request runs under (`docs/plans/page-load-standard/design.md`
//! §4).
//!
//! A request's class decides three things downstream: the tenant pool lane
//! the `Tenant` extractor hands the handler (`tenancy::extractor`), the
//! transaction shape `Tenant::begin_member_scoped` opens (`tenancy::scope`:
//! `BEGIN READ ONLY` and a 1 s statement timeout on `PageRead`), and how the
//! admission layer counts and sheds it (`admission`). It is also a tag on the
//! `http_request` metric, the access log and `Server-Timing`.
//!
//! # How a request gets its class
//!
//! The class is a property of one (method, route template), declared where
//! the route is mounted: [`classify`] registers the pair in a process-wide
//! table, and [`RouteClass::of`] reads that table through the request method
//! and axum's [`MatchedPath`]. A route mounted with plain `.route(...)` is
//! `Other`. Method is part of the key because one template can carry a read
//! and a write (`GET /v1/graphs` lists, `POST /v1/graphs` creates), and only
//! the read may run in a read-only transaction.
//!
//! A table keyed by the matched template, rather than a `route_layer` that
//! stamps an extension, because the layers that need the class (admission,
//! metrics, the access log) run OUTSIDE the route's own layers and would never
//! see the stamp. `MatchedPath` is set by the router before any middleware
//! runs, so every layer and every extractor reads the same answer. An explicit
//! `RouteClass` extension, when present, wins over the table; tests use that
//! to classify a probe route without mounting it.

use std::sync::LazyLock;

use axum::extract::MatchedPath;
use axum::handler::Handler;
use axum::http::{Extensions, Method};
use axum::routing::{on, MethodFilter};
use axum::Router;
use dashmap::DashMap;

/// See the module doc. `Other` is the default for every route that does not
/// opt into another class at its mount.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum RouteClass {
    /// Indexed lookups of stored read models and other already-cheap page
    /// reads. Page pool lane, 1 s statement timeout, `BEGIN READ ONLY`.
    /// Never shed by the in-flight cap.
    PageRead,
    /// Long-lived SSE (ask streams). Work lane; the in-flight cap is
    /// observe-only because a stream holds a connection briefly and a
    /// request slot for minutes.
    Stream,
    /// Everything else (interactive reads, mutations, Library pages). Work
    /// lane, 10 s statement timeout, in-flight cap enforced when
    /// `CONCURRENCY_ENFORCE` is on.
    Other,
}

impl RouteClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            RouteClass::PageRead => "page_read",
            RouteClass::Stream => "stream",
            RouteClass::Other => "other",
        }
    }

    /// Postgres `statement_timeout` for one request transaction of this
    /// class, in milliseconds as text (`set_config` takes text; a bare
    /// integer is read as ms). `PageRead` is an indexed read of a stored
    /// row: one that takes a second is a lock or a missing index, and failing
    /// it fast is the signal (design §11).
    pub const fn statement_timeout_ms(self) -> &'static str {
        match self {
            RouteClass::PageRead => "1000",
            RouteClass::Stream | RouteClass::Other => "10000",
        }
    }

    /// The class of a request: an explicit `RouteClass` extension if one is
    /// present, else the class registered for (`method`, matched template),
    /// else `Other`.
    pub fn of(method: &Method, extensions: &Extensions) -> RouteClass {
        if let Some(class) = extensions.get::<RouteClass>() {
            return *class;
        }
        extensions
            .get::<MatchedPath>()
            .and_then(|m| TABLE.get(&key(method, m.as_str())).map(|c| *c))
            .unwrap_or(RouteClass::Other)
    }
}

/// `"GET /v1/me"`: method, one space, the full matched template (as axum's
/// `MatchedPath` renders it, including any nest prefix) → class. Populated at
/// router build, read per request.
static TABLE: LazyLock<DashMap<String, RouteClass>> = LazyLock::new(DashMap::new);

fn key(method: &Method, template: &str) -> String {
    format!("{method} {template}")
}

/// The prefix `lib.rs` nests `v1_routes()` under. Routes declared there are
/// written relative to it, so [`classify`] prepends it when registering.
pub const V1_PREFIX: &str = "/v1";

/// Mount `handler` for `method` at `path` (a `/v1`-relative literal, as
/// every entry in `v1_routes()` is written) under `class`. Returns a
/// one-route router to `.merge` into `v1_routes()`; axum merges it with any
/// other method already mounted on the same path.
pub fn classify<H, T, S>(
    class: RouteClass,
    method: Method,
    path: &'static str,
    handler: H,
) -> Router<S>
where
    H: Handler<T, S>,
    T: 'static,
    S: Clone + Send + Sync + 'static,
{
    register(&method, &format!("{V1_PREFIX}{path}"), class);
    if method == Method::GET {
        register(&Method::HEAD, &format!("{V1_PREFIX}{path}"), class);
    }
    Router::new().route(path, on(filter(method), handler))
}

/// [`classify`] for a route mounted on the open (un-nested) router, where
/// `path` is already the full template (`/v1/email/prefill`).
pub fn classify_absolute<H, T, S>(
    class: RouteClass,
    method: Method,
    path: &'static str,
    handler: H,
) -> Router<S>
where
    H: Handler<T, S>,
    T: 'static,
    S: Clone + Send + Sync + 'static,
{
    register(&method, path, class);
    if method == Method::GET {
        register(&Method::HEAD, path, class);
    }
    Router::new().route(path, on(filter(method), handler))
}

fn filter(method: Method) -> MethodFilter {
    MethodFilter::try_from(method.clone())
        .unwrap_or_else(|_| panic!("{method} is not a method axum can route on"))
}

fn register(method: &Method, template: &str, class: RouteClass) {
    if let Some(prev) = TABLE.insert(key(method, template), class) {
        assert_eq!(
            prev,
            class,
            "{method} {template} registered under two classes ({} then {})",
            prev.as_str(),
            class.as_str()
        );
    }
}

/// Every registered `"METHOD /template"` and its class, sorted. For tests and
/// the route guard; not on any request path.
pub fn registered() -> Vec<(String, RouteClass)> {
    let mut out: Vec<_> = TABLE
        .iter()
        .map(|e| (e.key().clone(), *e.value()))
        .collect();
    out.sort();
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use axum::response::IntoResponse;
    use axum::routing::post;
    use tower::ServiceExt;

    async fn class_seen(router: Router, method: Method, uri: &str) -> String {
        let res = router
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(uri)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        String::from_utf8(
            axum::body::to_bytes(res.into_body(), 1 << 16)
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap()
    }

    async fn report(req: Request<Body>, next: axum::middleware::Next) -> axum::response::Response {
        let class = RouteClass::of(req.method(), req.extensions());
        let _ = next.run(req).await;
        class.as_str().into_response()
    }

    #[tokio::test]
    async fn a_classified_route_is_seen_by_an_outer_layer_through_the_nest_prefix() {
        let v1 = Router::new()
            .merge(classify(
                RouteClass::PageRead,
                Method::GET,
                "/rc-test/graphs",
                || async { "" },
            ))
            .route("/rc-test/graphs", post(|| async { "" }))
            .merge(classify(
                RouteClass::Stream,
                Method::POST,
                "/rc-test/stream",
                || async { "" },
            ))
            .route("/rc-test/plain/{id}", axum::routing::get(|| async { "" }));
        let router = Router::new()
            .nest("/v1", v1)
            .layer(axum::middleware::from_fn(report));
        assert_eq!(
            class_seen(router.clone(), Method::GET, "/v1/rc-test/graphs").await,
            "page_read"
        );
        assert_eq!(
            class_seen(router.clone(), Method::POST, "/v1/rc-test/graphs").await,
            "other"
        );
        assert_eq!(
            class_seen(router.clone(), Method::POST, "/v1/rc-test/stream").await,
            "stream"
        );
        assert_eq!(
            class_seen(router.clone(), Method::GET, "/v1/rc-test/plain/7").await,
            "other"
        );
        assert_eq!(
            class_seen(router, Method::GET, "/v1/rc-test/nope").await,
            "other"
        );
    }

    #[tokio::test]
    async fn an_extension_overrides_the_table() {
        let router = Router::new()
            .route("/rc-test/forced", axum::routing::get(|| async { "" }))
            .layer(axum::middleware::from_fn(report))
            .layer(axum::Extension(RouteClass::PageRead));
        assert_eq!(
            class_seen(router, Method::GET, "/rc-test/forced").await,
            "page_read"
        );
    }

    #[test]
    fn timeouts_follow_the_design_numbers() {
        assert_eq!(RouteClass::PageRead.statement_timeout_ms(), "1000");
        assert_eq!(RouteClass::Other.statement_timeout_ms(), "10000");
        assert_eq!(RouteClass::Stream.statement_timeout_ms(), "10000");
    }

    #[test]
    #[should_panic(expected = "registered under two classes")]
    fn one_method_and_template_cannot_carry_two_classes() {
        register(&Method::GET, "/v1/rc-test/twice", RouteClass::PageRead);
        register(&Method::GET, "/v1/rc-test/twice", RouteClass::Other);
    }
}
