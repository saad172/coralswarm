//! Routing grants for [`super::Destination::OrgShared`]. A grant is built only
//! by a lookup that reads the routing record an admin configured (a Slack
//! channel mapping, a Google Chat space route, a GitHub repo binding) through
//! the connector's own resolver, and it records that record and the oceans it
//! allows. [`OrgSharedGrant::verify_tx`] re-reads the record at the end of
//! the capture transaction, share-locked, just before the commit: a record
//! added, removed or re-pointed after the lookup refuses the write and rolls
//! the capture back, and an admin's re-point waits only for the instant
//! between that check and the commit, never for the capture's uploads.

use std::collections::HashSet;
use std::sync::Arc;

use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::connectors::github_backfill::RepoBinding;
use crate::routes::slack::{membership, primary_ocean, AccountPlane};
use crate::tenancy::TenantId;
use crate::AppState;

/// The admin-configured routing that allows a write into a shared org ocean.
/// The inner records have private fields: only the lookups below build one.
#[derive(Debug, Clone)]
pub(crate) enum OrgSharedGrant {
    /// A `slack_watched_channels` row, or the account default a channel with
    /// no row falls back to (`connectors::slack::resolve_channel_route`).
    SlackChannel(SlackChannel),
    /// A `google_chat_watched_spaces` route: a container space, or one
    /// member's mailbox opt-in.
    GoogleChatSpace(ChatSpace),
    /// A `github_watched_repos` binding whose chooser can still reach its
    /// ocean.
    GitHubRepo(GitHubRepo),
}

/// A routing row's `(mode, graph_id)`.
type Route = (String, Option<Uuid>);

/// How a grant re-checks that the owner of an ocean it writes to is still a
/// member of the account. Only the routes whose check asks it carry one: a
/// fan-out (every ocean is a member's own) and a Chat mailbox opt-in (the
/// opt-in's owner). A `single` route names its ocean on the locked row and
/// keeps [`Roster::NotKept`].
///
/// The capture transaction holds one pooled connection on the content plane.
/// Membership (`account_members`) is a control table, so where it lives
/// decides how it is read:
///
/// * On the consumer plane (`TenantId::Consumer`) the control database IS
///   the content database and the two pools are one pool. The read runs on
///   the capture's own connection: it sees the same snapshot as the lock,
///   and it never asks the pool for a second connection while holding one.
///   Doing that from every in-flight capture at once is a self-deadlock
///   under a burst (every connection held by a capture waiting for one more,
///   until `acquire_timeout`).
/// * On an org tenant (`TenantId::Org`) `account_members` is on another
///   database and cannot join the transaction. The grant keeps the roster as
///   of its build: for a fan-out, the member set the resolver already read
///   to pick the oceans (no second read); for a mailbox opt-in, one
///   membership read of its owner from the control pool. Each write then
///   checks its ocean's owner against that set. The control pool is a
///   different pool from the one the capture holds, and the read happens
///   outside the capture, so a burst of captures cannot hold the tenant pool
///   while waiting on control. A member removed during one pass (one tick,
///   one page, one event) still receives that pass; the next pass builds a
///   new grant.
#[derive(Debug, Clone)]
enum Roster {
    /// Not consulted: a `single` route, or a personal account, which is the
    /// user's own (`github_backfill::destination_reachable`'s rule).
    NotKept,
    /// `account_members` is in the capture's database: read in the capture.
    InCapture,
    /// `account_members` is on the control database: the members as of the
    /// grant's build.
    Snapshot(Arc<HashSet<Uuid>>),
}

#[derive(Debug, Clone)]
pub(crate) struct SlackChannel {
    account_id: Uuid,
    workspace_id: String,
    channel_id: String,
    /// The channel's row; `None` is the account default.
    route: Option<Route>,
    plane: AccountPlane,
    roster: Roster,
    namespace: String,
    oceans: Vec<Uuid>,
}

#[derive(Debug, Clone)]
pub(crate) struct ChatSpace {
    account_id: Uuid,
    space_id: String,
    owner_user_id: Option<Uuid>,
    route: Route,
    roster: Roster,
    namespace: String,
    oceans: Vec<Uuid>,
}

#[derive(Debug, Clone)]
pub(crate) struct GitHubRepo {
    account_id: Uuid,
    row_id: Uuid,
    owner_user_id: Uuid,
    namespace: String,
    oceans: Vec<Uuid>,
}

/// Why [`OrgSharedGrant::github_repo`] granted nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RepoRefusal {
    /// No active binding with a destination and a chooser.
    Unbound,
    /// The chooser can no longer reach the bound ocean.
    Unreachable { graph_id: Uuid, owner_user_id: Uuid },
    /// The reachability lookup itself failed; nothing is known about the
    /// binding, so the caller retries rather than pausing it.
    Lookup(String),
}

impl std::fmt::Display for RepoRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unbound => write!(f, "not an active binding to a destination ocean"),
            Self::Unreachable { graph_id, owner_user_id } => {
                write!(f, "destination ocean {graph_id} is no longer reachable by {owner_user_id}")
            }
            Self::Lookup(e) => write!(f, "grant lookup failed: {e}"),
        }
    }
}

impl OrgSharedGrant {
    /// Grant for a Slack channel, through `resolve_channel_route`: the grant
    /// records the very row the destination was materialized from. Also
    /// returns how many org members the fan-out could not map (zero for a
    /// single destination). `tenant` is the plane the capture will run on;
    /// see [`Roster`].
    pub(crate) async fn slack_channel(
        control_pool: &PgPool,
        tenant_pool: &PgPool,
        tenant: &TenantId,
        account_id: Uuid,
        plane: AccountPlane,
        workspace_id: &str,
        channel_id: &str,
    ) -> Result<(Self, usize), sqlx::Error> {
        use crate::connectors::slack::ChannelDestination;
        let (route, destination) = crate::connectors::slack::resolve_channel_route(
            control_pool, tenant_pool, account_id, plane, workspace_id, channel_id,
        )
        .await?;
        let (oceans, unmapped_members, roster) = match destination {
            ChannelDestination::Single(graph_id) => (vec![graph_id], 0, Roster::NotKept),
            ChannelDestination::FanOut { graphs, unmapped_members, members } => {
                (graphs, unmapped_members, Roster::for_fanout(tenant, plane, members))
            }
        };
        let grant = Self::SlackChannel(SlackChannel {
            account_id,
            workspace_id: workspace_id.to_string(),
            channel_id: channel_id.to_string(),
            route,
            plane,
            roster,
            namespace: format!("slack:workspace:{workspace_id}"),
            oceans,
        });
        Ok((grant, unmapped_members))
    }

    /// Grant for a routed Google Chat space: the container route
    /// (`owner_user_id = None`) or a member's mailbox opt-in. `Ok(None)` when
    /// the space has no active route. Also returns how many members a
    /// fan-out could not map. `tenant` is the plane the capture will run on;
    /// see [`Roster`].
    ///
    /// `single` and mailbox routes name their ocean. `fanout` resolves each
    /// mapped member's own ocean, and only on the Org plane: a fan-out row on
    /// the Personal plane would resolve "every member of a one-member
    /// account", so it is refused, as `pick_space_destination` refuses to
    /// create one.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn google_chat_space(
        control_pool: &PgPool,
        content_pool: &PgPool,
        tenant: &TenantId,
        account_id: Uuid,
        plane: AccountPlane,
        space_id: &str,
        owner_user_id: Option<Uuid>,
    ) -> Result<Option<(Self, usize)>, String> {
        let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(content_pool, account_id)
            .await
            .map_err(|e| format!("chat route scope: {e}"))?;
        let route = chat_route_tx(&mut tx, account_id, space_id, owner_user_id, Lock::No)
            .await
            .map_err(|e| format!("chat route: {e}"))?;
        tx.rollback().await.ok();
        let Some((mode, graph_id)) = route else {
            return Ok(None);
        };
        let (oceans, unmapped_members, roster) = match (mode.as_str(), graph_id, owner_user_id) {
            ("single", Some(graph_id), None) => (vec![graph_id], 0, Roster::NotKept),
            ("single", Some(graph_id), Some(owner)) => {
                let roster = Roster::for_owner(control_pool, tenant, plane, account_id, owner)
                    .await
                    .map_err(|e| format!("chat route roster: {e}"))?;
                (vec![graph_id], 0, roster)
            }
            ("fanout", _, None) => {
                if plane != AccountPlane::Org {
                    return Err(format!(
                        "google_chat: space {space_id} is marked fanout on the Personal plane, which \
                         pick_space_destination refuses to create — refusing to resolve destinations for it"
                    ));
                }
                let fanout =
                    crate::connectors::google_chat::resolve_chat_fanout_destinations(control_pool, content_pool, account_id)
                        .await
                        .map_err(|e| format!("fan-out resolution for {space_id}: {e}"))?;
                (fanout.graphs, fanout.unmapped_members, Roster::for_fanout(tenant, plane, fanout.members))
            }
            // The table's archetype CHECK makes any other shape unwritable;
            // an impossible route grants nothing rather than panicking.
            _ => return Ok(None),
        };
        let grant = Self::GoogleChatSpace(ChatSpace {
            account_id,
            space_id: space_id.to_string(),
            owner_user_id,
            route: (mode, graph_id),
            roster,
            namespace: format!("google_chat:space:{space_id}"),
            oceans,
        });
        Ok(Some((grant, unmapped_members)))
    }

    /// Grant for a GitHub watch row the caller has loaded: active, bound to
    /// an ocean, and that ocean still reachable by the member who chose it
    /// (`github_backfill::destination_reachable`, which on an org tenant also
    /// confirms the chooser is still a member). The chooser is
    /// [`Self::github_chooser`]; the binding's writes are attributed to them.
    /// [`Self::verify_tx`] re-reads the row under the lock, so the loaded row
    /// is not re-read here.
    pub(crate) async fn github_repo(
        state: &Arc<AppState>,
        tenant: &TenantId,
        pool: &PgPool,
        account_id: Uuid,
        binding: &RepoBinding,
    ) -> Result<Self, RepoRefusal> {
        let Some((graph_id, owner_user_id)) = binding.bound() else {
            return Err(RepoRefusal::Unbound);
        };
        let reachable =
            crate::connectors::github_backfill::destination_reachable(state, tenant, pool, account_id, graph_id, owner_user_id)
                .await
                .map_err(RepoRefusal::Lookup)?;
        if !reachable {
            return Err(RepoRefusal::Unreachable { graph_id, owner_user_id });
        }
        Ok(Self::GitHubRepo(GitHubRepo {
            account_id,
            row_id: binding.row_id,
            owner_user_id,
            namespace: format!("github:repository:{}", binding.repo_id),
            oceans: vec![graph_id],
        }))
    }

    /// The oceans this grant allows writes into.
    pub(crate) fn oceans(&self) -> &[Uuid] {
        match self {
            Self::SlackChannel(g) => &g.oceans,
            Self::GoogleChatSpace(g) => &g.oceans,
            Self::GitHubRepo(g) => &g.oceans,
        }
    }

    /// The account that owns the routing record and every ocean it allows.
    pub(crate) fn account_id(&self) -> Uuid {
        match self {
            Self::SlackChannel(g) => g.account_id,
            Self::GoogleChatSpace(g) => g.account_id,
            Self::GitHubRepo(g) => g.account_id,
        }
    }

    /// The provider namespace every source written under this grant lives in.
    pub(crate) fn namespace(&self) -> &str {
        match self {
            Self::SlackChannel(g) => &g.namespace,
            Self::GoogleChatSpace(g) => &g.namespace,
            Self::GitHubRepo(g) => &g.namespace,
        }
    }

    /// For a GitHub binding, the member who chose its destination, whom its
    /// writes are attributed to. `None` for the other grants.
    pub(crate) fn github_chooser(&self) -> Option<Uuid> {
        match self {
            Self::GitHubRepo(g) => Some(g.owner_user_id),
            Self::SlackChannel(_) | Self::GoogleChatSpace(_) => None,
        }
    }

    /// Does the routing record still exist as the lookup read it, and still
    /// allow `ocean`? Runs at the end of the capture transaction, scoped to
    /// [`Self::account_id`], after the rows and uploads and just before the
    /// commit, and leaves the record share-locked until that commit: an
    /// admin's re-point or delete waits for the commit, so a write never
    /// lands under a routing that no longer exists, and a capture whose
    /// routing changed while it ran is refused and rolled back. Nothing
    /// before the check holds the lock, so the row's own bookkeeping
    /// (cursors, attempt stamps, error codes) is not queued behind a
    /// capture's uploads.
    ///
    /// What the lookup derived from the record is re-derived here, on the
    /// capture's own connection: a fan-out ocean must still be a current
    /// member's own primary ocean, a mailbox opt-in's owner must still be a
    /// member ([`Roster`] says how membership is read), and a repo binding's
    /// chooser must still reach the ocean. The identity mapping behind a
    /// fan-out is not re-done: it is two reads on two databases, and losing
    /// an identity link does not change who may receive the write; losing
    /// membership does. The GitHub chooser's membership was confirmed when
    /// the grant was built (`destination_reachable`), once per tick or event.
    pub(super) async fn verify_tx(&self, tx: &mut Transaction<'static, Postgres>, ocean: Uuid) -> Result<bool, sqlx::Error> {
        if !self.oceans().contains(&ocean) {
            return Ok(false);
        }
        match self {
            Self::SlackChannel(g) => {
                let live = match &g.route {
                    Some(route) => {
                        slack_route_tx(tx, g.account_id, &g.workspace_id, &g.channel_id, Lock::Share).await?.as_ref()
                            == Some(route)
                    }
                    // No row to lock: a channel on the account default. A
                    // row added before this point refuses the write; one
                    // added after it lands beside a capture that was routed
                    // by the default in force when it began.
                    None => slack_route_tx(tx, g.account_id, &g.workspace_id, &g.channel_id, Lock::No).await?.is_none(),
                };
                if !live {
                    return Ok(false);
                }
                match (&g.route, g.plane) {
                    // The locked row names the ocean.
                    (Some((mode, _)), _) if mode == "single" => Ok(true),
                    (None, AccountPlane::Personal) => primary_ocean_tx(tx, g.account_id, ocean).await,
                    _ => fanout_ocean_tx(tx, &g.roster, g.account_id, ocean).await,
                }
            }
            Self::GoogleChatSpace(g) => {
                if chat_route_tx(tx, g.account_id, &g.space_id, g.owner_user_id, Lock::Share).await?.as_ref() != Some(&g.route) {
                    return Ok(false);
                }
                match (g.route.0.as_str(), g.owner_user_id) {
                    ("single", Some(owner)) => g.roster.still_a_member(tx, g.account_id, owner).await,
                    ("single", None) => Ok(true),
                    _ => fanout_ocean_tx(tx, &g.roster, g.account_id, ocean).await,
                }
            }
            Self::GitHubRepo(g) => {
                let row: Option<(Option<Uuid>, Option<Uuid>)> = sqlx::query_as(
                    "SELECT graph_id, owner_user_id FROM github_watched_repos \
                     WHERE id = $1 AND account_id = $2 AND active FOR SHARE",
                )
                .bind(g.row_id)
                .bind(g.account_id)
                .fetch_optional(&mut **tx)
                .await?;
                if row != Some((Some(ocean), Some(g.owner_user_id))) {
                    return Ok(false);
                }
                // The chooser's reachability, as `destination_reachable`
                // asks it: a SECURITY DEFINER predicate, unscoped on purpose.
                sqlx::query_scalar("SELECT public.resolve_tenant_graph_reachable_by_user($1, $2, $3)")
                    .bind(ocean)
                    .bind(g.account_id)
                    .bind(g.owner_user_id)
                    .fetch_one(&mut **tx)
                    .await
            }
        }
    }
}

#[cfg(test)]
impl OrgSharedGrant {
    /// A Chat fan-out grant over `oceans`, without resolving member
    /// identities. [`OrgSharedGrant::verify_tx`] still requires the space's
    /// `fanout` route row and each ocean to be a member's own; the roster is
    /// not re-read.
    pub(crate) fn google_chat_fanout_for_test(account_id: Uuid, space_id: &str, oceans: Vec<Uuid>) -> Self {
        Self::GoogleChatSpace(ChatSpace {
            account_id,
            space_id: space_id.to_string(),
            owner_user_id: None,
            route: ("fanout".to_string(), None),
            roster: Roster::NotKept,
            namespace: format!("google_chat:space:{space_id}"),
            oceans,
        })
    }
}

impl Roster {
    /// The roster for a fan-out route: `members` is the set the resolver
    /// intersected the identity mapping with, kept as the snapshot where the
    /// capture cannot read membership itself. See the type's doc.
    fn for_fanout(tenant: &TenantId, plane: AccountPlane, members: HashSet<Uuid>) -> Self {
        match (plane, tenant) {
            (AccountPlane::Personal, _) => Self::NotKept,
            (AccountPlane::Org, TenantId::Consumer) => Self::InCapture,
            (AccountPlane::Org, TenantId::Org(_)) => Self::Snapshot(Arc::new(members)),
        }
    }

    /// The roster for a route owned by one member (a Chat mailbox opt-in):
    /// only that member's membership is ever asked, so on an org tenant the
    /// snapshot is one read of it. See the type's doc.
    async fn for_owner(
        control_pool: &PgPool,
        tenant: &TenantId,
        plane: AccountPlane,
        account_id: Uuid,
        owner: Uuid,
    ) -> Result<Self, sqlx::Error> {
        match (plane, tenant) {
            (AccountPlane::Personal, _) => Ok(Self::NotKept),
            (AccountPlane::Org, TenantId::Consumer) => Ok(Self::InCapture),
            (AccountPlane::Org, TenantId::Org(_)) => {
                // `account_members` is a control-only table; the account
                // scope is the one `google_chat::is_current_account_member`
                // reads it under.
                let mut tx = crate::tenancy::scope::begin_account_scoped(control_pool, account_id)
                    .await
                    .map_err(|e| sqlx::Error::Protocol(format!("roster scope: {e}")))?;
                let member = membership::is_current_member(&mut *tx, account_id, owner).await?;
                tx.rollback().await.ok();
                Ok(Self::Snapshot(Arc::new(member.then_some(owner).into_iter().collect())))
            }
        }
    }

    async fn still_a_member(&self, tx: &mut Transaction<'static, Postgres>, account_id: Uuid, user_id: Uuid) -> Result<bool, sqlx::Error> {
        match self {
            Self::NotKept => Ok(true),
            Self::InCapture => membership::is_current_member(&mut **tx, account_id, user_id).await,
            Self::Snapshot(members) => Ok(members.contains(&user_id)),
        }
    }
}

/// Whether to leave the routing row share-locked for the transaction.
#[derive(Clone, Copy)]
enum Lock {
    No,
    Share,
}

impl Lock {
    fn clause(self) -> &'static str {
        match self {
            Self::No => "",
            Self::Share => " FOR SHARE",
        }
    }
}

/// A Slack channel's `slack_watched_channels` row.
async fn slack_route_tx(
    tx: &mut Transaction<'static, Postgres>,
    account_id: Uuid,
    workspace_id: &str,
    channel_id: &str,
    lock: Lock,
) -> Result<Option<Route>, sqlx::Error> {
    sqlx::query_as(&format!(
        "SELECT mode, graph_id FROM slack_watched_channels \
         WHERE account_id = $1 AND workspace_id = $2 AND channel_id = $3{}",
        lock.clause()
    ))
    .bind(account_id)
    .bind(workspace_id)
    .bind(channel_id)
    .fetch_optional(&mut **tx)
    .await
}

/// The active route of a Chat space.
async fn chat_route_tx(
    tx: &mut Transaction<'static, Postgres>,
    account_id: Uuid,
    space_id: &str,
    owner_user_id: Option<Uuid>,
    lock: Lock,
) -> Result<Option<Route>, sqlx::Error> {
    sqlx::query_as(&format!(
        "SELECT mode, graph_id FROM google_chat_watched_spaces \
         WHERE account_id = $1 AND space_id = $2 AND owner_user_id IS NOT DISTINCT FROM $3 \
           AND active AND mode IS NOT NULL{}",
        lock.clause()
    ))
    .bind(account_id)
    .bind(space_id)
    .bind(owner_user_id)
    .fetch_optional(&mut **tx)
    .await
}

/// Is `ocean` still the account's own primary ocean, the one
/// `routes::slack::resolve_primary_graph` names for a personal account
/// ([`primary_ocean::PERSONAL`])?
async fn primary_ocean_tx(tx: &mut Transaction<'static, Postgres>, account_id: Uuid, ocean: Uuid) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(&format!(
        "SELECT EXISTS(SELECT 1 FROM graphs WHERE id = $1 AND account_id = $2 AND {})",
        primary_ocean::PERSONAL
    ))
    .bind(ocean)
    .bind(account_id)
    .fetch_one(&mut **tx)
    .await
}

/// Is `ocean` still a current member's own primary ocean
/// ([`primary_ocean::MEMBER`], the predicate both fan-out resolvers bind)?
async fn fanout_ocean_tx(
    tx: &mut Transaction<'static, Postgres>,
    roster: &Roster,
    account_id: Uuid,
    ocean: Uuid,
) -> Result<bool, sqlx::Error> {
    let owner: Option<Uuid> = sqlx::query_scalar(&format!(
        "SELECT owner_user_id FROM graphs WHERE id = $1 AND account_id = $2 AND {}",
        primary_ocean::MEMBER
    ))
    .bind(ocean)
    .bind(account_id)
    .fetch_optional(&mut **tx)
    .await?;
    match owner {
        Some(owner) => roster.still_a_member(tx, account_id, owner).await,
        None => Ok(false),
    }
}
