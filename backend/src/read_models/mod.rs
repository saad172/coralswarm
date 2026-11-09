//! Typed stored page answers. Add a domain module and one registry entry.
//! Request reads never compute. Worker computation is a separate member-scoped,
//! read-only repeatable snapshot; bookkeeping is the only system-scoped work.
use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{PgConnection, Postgres, Transaction};
use std::{collections::BTreeMap, future::Future, pin::Pin, time::Duration};
use uuid::Uuid;

pub mod access;
pub mod blobs;
pub mod library;
pub mod billing;
pub mod dirty;
pub mod failure;
pub mod chats;
pub mod connectors;
pub mod home;
pub mod oceans;
pub mod oceans_pages;
pub mod topics;
pub mod chats_detail;
pub mod org;
pub mod registry;
pub mod store;
pub mod today;
pub type Error = Box<dyn std::error::Error + Send + Sync>;
#[derive(Debug)]
pub struct NotFound(pub &'static str);
impl std::fmt::Display for NotFound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for NotFound {}
/// A verified selector is malformed or belongs to a different selected object.
#[derive(Debug)]
pub struct BadRequest(pub &'static str);
impl std::fmt::Display for BadRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str(self.0) }
}
impl std::error::Error for BadRequest {}
/// The actor cannot address a scope: its stored identity (membership, graphs
/// or viewer class) does not match the actor's live identity. A typed
/// refusal, so the worker logs `scope_unaddressable` and the variant's
/// [`Unaddressable::as_str`] without any error text, and backs reversible refusals off. A strictly older
/// control revision proves permanent obsolescence and permits retirement. A request path answers it as a denied
/// scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unaddressable {
    /// The certified monotonic control revision has superseded this address.
    ControlRevision,
    /// The compute actor is no longer a live member of the org.
    Membership,
    /// A graph the parameters name is outside the actor's visible set.
    ScopeGraphs,
    /// The live viewer derives a different scope key or graph set.
    ViewerClass,
    /// The claim's graph set is unsorted, duplicated or no longer visible.
    ClaimedGraphs,
}
impl Unaddressable {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ControlRevision => "control_revision",
            Self::Membership => "membership",
            Self::ScopeGraphs => "scope_graphs",
            Self::ViewerClass => "viewer_class",
            Self::ClaimedGraphs => "claimed_graphs",
        }
    }
}
impl std::fmt::Display for Unaddressable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "scope unaddressable: {}", self.as_str())
    }
}
impl std::error::Error for Unaddressable {}

pub type ComputeFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, Error>> + Send + 'a>>;

pub trait Params: Serialize + DeserializeOwned + Clone + Send + Sync + 'static {
    /// Resolve indexed identity preferences into canonical stored parameters.
    /// This may not read or compute content. Workers receive the normalized value.
    fn normalize<'a>(self, _tx: &'a mut PgConnection, _user_id: Uuid) -> ComputeFuture<'a, Self>
    where
        Self: Sized,
    {
        Box::pin(async move { Ok(self) })
    }
    /// Parse query parameters plus named path parameters. Path wins only when
    /// the same value is present; conflicting values must be rejected.
    fn parse(
        path: BTreeMap<String, String>,
        mut query: BTreeMap<String, String>,
    ) -> Result<Self, Error>
    where
        Self: Sized,
    {
        for (key, value) in path {
            if query.get(&key).is_some_and(|q| q != &value) {
                return Err("conflicting path/query parameter".into());
            }
            query.insert(key, value);
        }
        Ok(serde_urlencoded::from_str(&serde_urlencoded::to_string(
            query,
        )?)?)
    }
}
/// Typed URL parsing shared by models that need additional validation.
pub(crate) fn parse_params<P: DeserializeOwned>(
    path: BTreeMap<String, String>,
    mut query: BTreeMap<String, String>,
) -> Result<P, Error> {
    for (key, value) in path {
        if query.get(&key).is_some_and(|q| q != &value) {
            return Err("conflicting path/query parameter".into());
        }
        query.insert(key, value);
    }
    Ok(serde_urlencoded::from_str(&serde_urlencoded::to_string(
        query,
    )?)?)
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NoParams {}
impl Params for NoParams {}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ViewerClass {
    Shared { graphs: Vec<Uuid> },
    Windowed { user_id: Uuid, graphs: Vec<Uuid> },
}
impl ViewerClass {
    pub fn graphs(&self) -> &[Uuid] {
        match self {
            Self::Shared { graphs } | Self::Windowed { graphs, .. } => graphs,
        }
    }
    pub fn user_id(&self) -> Option<Uuid> {
        match self {
            Self::Shared { .. } => None,
            Self::Windowed { user_id, .. } => Some(*user_id),
        }
    }
}
/// Fields private: a viewer is minted from the authenticated actor's database
/// visibility, never deserialized from HTTP or trusted from stored metadata.
#[derive(Clone, Debug)]
pub struct Viewer {
    pub(crate) account_id: Uuid,
    pub(crate) user_id: Uuid,
    pub(crate) windows: BTreeMap<Uuid, Option<DateTime<Utc>>>,
    pub(crate) control_revision: Option<i64>,
    pub(crate) control_freshness: i64,
}
impl Viewer {
    pub async fn load(
        tx: &mut PgConnection,
        account_id: Uuid,
        user_id: Uuid,
    ) -> Result<Self, Error> {
        let rows: Vec<(Uuid, Option<DateTime<Utc>>)> = sqlx::query_as(
            "SELECT graph_id,history_floor FROM read_model_visible_windows() ORDER BY graph_id",
        )
        .fetch_all(tx)
        .await
        .map_err(crate::store::StoreError::from)?;
        Ok(Self {
            account_id,
            user_id,
            windows: rows.into_iter().collect(),
            control_revision: None,
            control_freshness: 0,
        })
    }
    pub fn visible_graphs(&self) -> Vec<Uuid> {
        self.windows.keys().copied().collect()
    }
    /// The revision lives beside the control facts and changes in their writer's
    /// transaction. It fences first fill and failed cross-plane invalidation.
    pub async fn certify_control(&mut self, tx: &mut PgConnection) -> Result<(), Error> {
        let (authority, freshness): (i64, i64) = sqlx::query_as(
            "SELECT COALESCE((SELECT revision FROM read_model_control_revisions WHERE account_id=$1),0), COALESCE((SELECT freshness FROM read_model_control_revisions WHERE account_id=$1),0)",
        ).bind(self.account_id).fetch_one(tx).await.map_err(crate::store::StoreError::from)?;
        self.control_revision = Some(authority);
        self.control_freshness = freshness;
        Ok(())
    }
    pub fn scope<M: ReadModel>(&self, params: M::Params) -> Result<Scope<M::Params>, Error> {
        let control_revision = if M::CONTROL_FACTS {
            self.control_revision
                .ok_or("control facts have not been certified")?
        } else {
            0
        };
        let mut graphs = M::graphs(self, &params);
        graphs.sort_unstable();
        graphs.dedup();
        // The one genuine refusal; an uncertified viewer above or a failed
        // serialization below keeps its own kind.
        if graphs.iter().any(|g| !self.windows.contains_key(g)) {
            return Err(Box::new(Unaddressable::ScopeGraphs));
        }
        let windowed = M::PER_USER || graphs.iter().any(|g| self.windows[g].is_some());
        let class = if windowed {
            ViewerClass::Windowed {
                user_id: self.user_id,
                graphs,
            }
        } else {
            ViewerClass::Shared { graphs }
        };
        let floors: BTreeMap<_, _> = class
            .graphs()
            .iter()
            .map(|g| (*g, self.windows[g]))
            .collect();
        // Full SHA256, account and typed version; recursive canonical JSON also
        // sorts nested maps. Neither viewer nor params can collide by concatenation.
        let mut identity =
            serde_json::json!([M::NAME, M::VERSION, self.account_id, class, floors, params]);
        if M::CONTROL_FACTS {
            identity
                .as_array_mut()
                .expect("scope identity array")
                .push(serde_json::json!(control_revision));
        }
        let identity = canonical(identity);
        let key = hex::encode(Sha256::digest(serde_json::to_vec(&identity)?));
        Ok(Scope {
            class,
            params,
            key,
            account_id: self.account_id,
            compute_as: self.user_id,
            control_revision,
            control_freshness: if M::CONTROL_FACTS {
                self.control_freshness
            } else {
                0
            },
        })
    }
}
fn canonical(v: Value) -> Value {
    match v {
        Value::Object(m) => Value::Object(
            m.into_iter()
                .map(|(k, v)| (k, canonical(v)))
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .collect(),
        ),
        Value::Array(v) => Value::Array(v.into_iter().map(canonical).collect()),
        v => v,
    }
}
#[derive(Clone, Debug)]
pub struct Scope<P: Params> {
    pub class: ViewerClass,
    pub params: P,
    pub(crate) key: String,
    pub(crate) account_id: Uuid,
    pub(crate) compute_as: Uuid,
    pub(crate) control_revision: i64,
    pub(crate) control_freshness: i64,
}
impl<P: Params> Scope<P> {
    pub fn key(&self) -> &str {
        &self.key
    }
}

/// Indexed owning-account bootstrap only; visibility is still rederived from
/// the verified actor before any stored answer can be addressed.
#[derive(Clone,Copy)]
pub enum AccountAnchor { Atom(Uuid), Node(Uuid), Conversation(Uuid) }
pub(crate) async fn anchored_account(tx:&mut PgConnection, anchor:AccountAnchor)->Result<Option<Uuid>,Error> {
    Ok(match anchor {
        AccountAnchor::Atom(id)=>sqlx::query_scalar("SELECT * FROM resolve_atoms_account_id($1)").bind(&[id][..]).fetch_optional(tx).await?,
        AccountAnchor::Node(id)=>sqlx::query_scalar("SELECT read_model_node_account($1)").bind(id).fetch_one(tx).await?,
        AccountAnchor::Conversation(id)=>sqlx::query_scalar("SELECT read_model_conversation_account($1)").bind(id).fetch_one(tx).await?,
    })
}

pub trait ReadModel: Send + Sync + 'static {
    /// Preserve feature availability at the stored API boundary.
    fn enabled() -> bool {
        true
    }
    /// Some legacy resources conceal an explicitly named inaccessible graph.
    const HIDE_DENIED_SCOPE: bool = false;
    const NAME: &'static str;
    const PATH: &'static str;
    const VERSION: i32;
    const PER_USER: bool = false;
    /// Certify the account-wide metering sentinel, independent of content graphs.
    const ACCOUNT_FACTS: bool = false;
    const ACCESS: access::AccessPolicy = access::AccessPolicy::Member;
    /// Open a second, member-scoped READ ONLY transaction for control facts.
    /// No compute body receives a writable control pool.
    const CONTROL_FACTS: bool = false;
    const DEBOUNCE: Duration = Duration::from_secs(30);
    const MAX_DELAY: Duration = Duration::from_secs(60);
    const COMPUTE_TIMEOUT: Duration = Duration::from_secs(30);
    type Params: Params;
    type Payload: Serialize + DeserializeOwned + Send + Sync + 'static;
    fn account_anchor(_params: &Self::Params) -> Option<AccountAnchor> { None }
    fn account_graph(_params: &Self::Params) -> Option<Uuid> {
        None
    }
    fn graphs(viewer: &Viewer, _params: &Self::Params) -> Vec<Uuid> {
        viewer.visible_graphs()
    }
    fn compute<'a>(cx: ComputeCx<'a>, params: &'a Self::Params)
        -> ComputeFuture<'a, Self::Payload>;
    fn empty(params: &Self::Params) -> Self::Payload;
    /// Bounded, indexed request-owned controls on an already certified payload.
    /// Runs in the existing read-only page transaction, never on a withheld or
    /// missing row. Must not recompute content or change its freshness certificate.
    fn overlay_current<'a>(
        _tx: &'a mut PgConnection,
        _scope: &'a Scope<Self::Params>,
        _payload: &'a mut Self::Payload,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
    /// Strip optional privileged facts using current verified request authority.
    fn present(_payload:&mut Self::Payload,_access:access::ReadAccess) {}
    fn dirty_triggers() -> &'static [dirty::DirtyTrigger];
}
/// Owned context with one borrow lifetime: compute can reborrow its transaction
/// across awaits without tying a mutable reference to its own container lifetime.
pub struct ComputeCx<'a> {
    pub tx: &'a mut Transaction<'static, Postgres>,
    pub blobs: Option<&'a blobs::BlobReader<'a>>,
    pub control_tx: Option<&'a mut Transaction<'static, Postgres>>,
    pub account_id: Uuid,
    pub compute_as: Uuid,
    pub graphs: &'a [Uuid],
    pub now: DateTime<Utc>,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Stored<P> {
    pub model: String,
    pub version: i32,
    pub payload: P,
    pub computed_at: Option<DateTime<Utc>>,
    pub stale: bool,
    pub refresh_eta: Option<DateTime<Utc>>,
    /// The row is behind a reduction or an invalidating fence (row-level
    /// `invalidated_xid`, or `read_model_fence_state` 0). Not on the wire:
    /// the client sees `stale`. The serving paths use it to owe the immediate
    /// recompute the withheld path used to owe, so a read after the caller's
    /// own reducing write does not wait out the dirty debounce.
    #[serde(skip)]
    pub behind: bool,
}
impl<P> Stored<P> {
    /// Whether the serving path owes an enqueue, settling the ETA it then
    /// promises. Owed when the row was never computed or is withheld (no
    /// `computed_at`), when it is served stale but `behind` (the recompute
    /// otherwise waits for the dirty debounce, up to `MAX_DELAY`), or when it
    /// is stale with no ETA at all. `store::enqueue` backdates
    /// `dirty_bumped_at`, so the row is due at once: a behind row's debounce
    /// ETA is replaced by the 5 s floor. A withheld row keeps `decode`'s ETA,
    /// which carries any failure backoff an automatic enqueue cannot clear.
    pub fn owe_refresh(&mut self, now: DateTime<Utc>) -> bool {
        let owed =
            self.computed_at.is_none() || self.behind || (self.stale && self.refresh_eta.is_none());
        if owed {
            let soon = now + chrono::Duration::seconds(5);
            if self.computed_at.is_some() && self.behind {
                self.refresh_eta = Some(self.refresh_eta.map_or(soon, |eta| eta.min(soon)));
            } else {
                self.refresh_eta.get_or_insert(soon);
            }
        }
        owed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oceans::{WorkspaceCounts as M, WorkspaceCountsParams as P};
    struct Personal;
    impl ReadModel for Personal {
        const NAME: &'static str = "personal_test";
        const PATH: &'static str = "/test";
        const VERSION: i32 = 1;
        const PER_USER: bool = true;
        type Params = P;
        type Payload = crate::store::WorkspaceCounts;
        fn graphs(viewer: &Viewer, _p: &P) -> Vec<Uuid> {
            let mut gs = viewer.visible_graphs();
            gs.reverse();
            gs.extend(gs.clone());
            gs
        }
        fn compute<'a>(cx: ComputeCx<'a>, p: &'a P) -> ComputeFuture<'a, Self::Payload> {
            M::compute(cx, p)
        }
        fn empty(p: &P) -> Self::Payload {
            M::empty(p)
        }
        fn dirty_triggers() -> &'static [dirty::DirtyTrigger] {
            &[]
        }
    }
    struct Version2;
    impl ReadModel for Version2 {
        const NAME: &'static str = M::NAME;
        const PATH: &'static str = M::PATH;
        const VERSION: i32 = 2;
        type Params = P;
        type Payload = crate::store::WorkspaceCounts;
        fn graphs(v: &Viewer, p: &P) -> Vec<Uuid> {
            M::graphs(v, p)
        }
        fn compute<'a>(cx: ComputeCx<'a>, p: &'a P) -> ComputeFuture<'a, Self::Payload> {
            M::compute(cx, p)
        }
        fn empty(p: &P) -> Self::Payload {
            M::empty(p)
        }
        fn dirty_triggers() -> &'static [dirty::DirtyTrigger] {
            M::dirty_triggers()
        }
    }
    #[test]
    fn scope_identity_is_canonical_and_bound_to_permissions() {
        let a = Uuid::from_u128(1);
        let b = Uuid::from_u128(2);
        let u = Uuid::from_u128(3);
        let v = Viewer {
            account_id: a,
            user_id: u,
            windows: [(b, None), (a, None)].into(),
            control_revision: None,
            control_freshness: 0,
        };
        let scope = v.scope::<M>(P { graph: b }).unwrap();
        assert!(matches!(scope.class, ViewerClass::Shared { .. }));
        assert_ne!(
            scope.key(),
            v.scope::<Version2>(P { graph: b }).unwrap().key()
        );
        assert_eq!(
            v.scope::<Personal>(P { graph: b }).unwrap().class.graphs(),
            &[a, b]
        );
        let other = Viewer {
            user_id: b,
            ..v.clone()
        };
        assert_eq!(scope.key(), other.scope::<M>(P { graph: b }).unwrap().key());
        assert_ne!(scope.key(), v.scope::<M>(P { graph: a }).unwrap().key());
        let mut floored = v.clone();
        floored.windows.insert(b, Some(Utc::now()));
        let narrow = floored.scope::<M>(P { graph: b }).unwrap();
        assert!(matches!(narrow.class, ViewerClass::Windowed { .. }));
        assert_ne!(scope.key(), narrow.key());
        floored
            .windows
            .insert(b, Some(Utc::now() + chrono::Duration::days(1)));
        assert_ne!(
            narrow.key(),
            floored.scope::<M>(P { graph: b }).unwrap().key()
        );
        assert_ne!(
            v.scope::<Personal>(P { graph: b }).unwrap().key(),
            other.scope::<Personal>(P { graph: b }).unwrap().key()
        );
        assert_ne!(
            scope.key(),
            Viewer {
                account_id: b,
                ..v.clone()
            }
            .scope::<M>(P { graph: b })
            .unwrap()
            .key()
        );
        assert_eq!(
            canonical(serde_json::from_str(r#"{"z":{"b":2,"a":1},"a":0}"#).unwrap()),
            canonical(serde_json::from_str(r#"{"a":0,"z":{"a":1,"b":2}}"#).unwrap())
        );
    }
    /// The serving paths owe an enqueue for a never-computed, withheld, behind
    /// or ETA-less stale row, and promise the ETA the enqueue makes true. A
    /// behind row served stale carries the trigger's debounce ETA, which the
    /// enqueue (due at once) replaces with the 5 s floor; a withheld row keeps
    /// the backoff ETA an automatic enqueue cannot clear.
    #[test]
    fn owe_refresh_for_behind_rows_and_withheld_rows() {
        let now = Utc::now();
        let soon = now + chrono::Duration::seconds(5);
        let later = now + chrono::Duration::seconds(30);
        let stored = |computed: bool, stale: bool, behind: bool, eta: Option<DateTime<Utc>>| Stored {
            model: "t".into(),
            version: 1,
            payload: (),
            computed_at: computed.then_some(now - chrono::Duration::seconds(60)),
            stale,
            refresh_eta: eta,
            behind,
        };
        // Never computed: owed, ETA inserted.
        let mut s = stored(false, true, false, None);
        assert!(s.owe_refresh(now));
        assert_eq!(s.refresh_eta, Some(soon));
        // Withheld in failure backoff: owed, the backoff ETA stands.
        let backoff = now + chrono::Duration::seconds(600);
        let mut s = stored(false, true, true, Some(backoff));
        assert!(s.owe_refresh(now));
        assert_eq!(s.refresh_eta, Some(backoff));
        // Served stale and behind, with the debounce ETA: owed, ETA pulled in.
        let mut s = stored(true, true, true, Some(later));
        assert!(s.owe_refresh(now));
        assert_eq!(s.refresh_eta, Some(soon));
        // Served stale and behind, already due: owed, the earlier ETA stands.
        let past = now - chrono::Duration::seconds(1);
        let mut s = stored(true, true, true, Some(past));
        assert!(s.owe_refresh(now));
        assert_eq!(s.refresh_eta, Some(past));
        // Served stale, not behind, the refresher already owes it: nothing.
        let mut s = stored(true, true, false, Some(later));
        assert!(!s.owe_refresh(now));
        assert_eq!(s.refresh_eta, Some(later));
        // Served stale without any ETA (aged past MAX_DELAY): owed.
        let mut s = stored(true, true, false, None);
        assert!(s.owe_refresh(now));
        assert_eq!(s.refresh_eta, Some(soon));
        // Current: nothing.
        let mut s = stored(true, false, false, None);
        assert!(!s.owe_refresh(now));
        assert_eq!(s.refresh_eta, None);
    }
}

#[cfg(feature = "e2e")]
static COMPUTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
#[cfg(feature = "e2e")]
static REQUEST_COMPUTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
#[cfg(feature = "e2e")]
pub(crate) fn note_compute() {
    use std::sync::atomic::Ordering;
    COMPUTES.fetch_add(1, Ordering::Relaxed);
    if crate::request_timing::current_class() == crate::route_class::RouteClass::PageRead {
        REQUEST_COMPUTES.fetch_add(1, Ordering::Relaxed);
    }
}
#[cfg(feature = "e2e")]
pub(crate) fn compute_counts() -> Value {
    use std::sync::atomic::Ordering;
    serde_json::json!({"worker_computes":COMPUTES.load(Ordering::Relaxed),"request_computes":REQUEST_COMPUTES.load(Ordering::Relaxed)})
}
