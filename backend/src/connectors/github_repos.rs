//! GitHub repo selection / destination-gating (docs/connectors/github.md
//! §3.4) — the per-repo personal/team/org destination picker, and
//! repo-mirrored team creation + auto-seat (contract I4).
//!
//! Gated **by role**, per the founder's 2026-09-21 decision recorded in the
//! design doc: personal is always open; an EXISTING team needs the chooser
//! to administer it (or they create a NEW one, which makes them its first
//! admin); the org ocean needs a CoralSwarm org admin. This is a deliberate
//! departure from Slack's own destination rule
//! (`connectors::slack_channel_actions::SetDestinationError::OrgOcean` —
//! Slack refuses the org ocean unconditionally) — see this module's own
//! [`DestinationError::OrgRequiresAdmin`] variant, which is NOT that type
//! reused: GitHub's org-ocean refusal is conditional (admin-gated), Slack's
//! is absolute, and folding the two into one enum would make a future Slack
//! change silently affect GitHub's gate or vice versa.

use sqlx::PgPool;
use uuid::Uuid;

use super::github::GithubApiError;
use crate::routes::slack::{account_plane, resolve_primary_graph, IngestCaller};
use crate::routes::slack_interactivity::{check_team_ocean_admin, TeamOceanCheck};
use crate::seats::{github_repo_source, record_seat_grant};

/// What a caller asked to route a repo to — the parsed request-body shape,
/// before any authorization has run.
#[derive(Debug, Clone)]
pub(crate) enum RequestedDestination {
    /// The chooser's own personal/member ocean. Always allowed (§3.4).
    Personal,
    /// An existing team ocean, by id — requires the chooser to administer it.
    ExistingTeam { team_id: Uuid },
    /// Create a brand-new repo-mirrored team ocean (name = `{owner}/{name}`),
    /// seating the chooser as its first admin, then auto-seating the repo's
    /// mapped collaborators (I4). Always allowed — creating a NEW team is
    /// how "I don't administer an existing one" is resolved, same shape as
    /// Slack's own "new channel-mirrored team" picker option.
    NewTeam,
    /// The account's org commons ocean — requires a CoralSwarm org admin.
    Org,
}

/// The result of a successful [`resolve_destination`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ResolvedDestination {
    pub(crate) graph_id: Uuid,
    pub(crate) kind: DestinationKind,
    /// `Some(team_id)` only when [`RequestedDestination::NewTeam`] actually
    /// created one this call — the caller uses this to know whether to run
    /// the auto-seat step (an EXISTING team is not re-seated on every pick;
    /// only creation seats the founding roster).
    pub(crate) newly_created_team: Option<Uuid>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DestinationKind {
    Personal,
    Team,
    Org,
}

impl DestinationKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            DestinationKind::Personal => "personal",
            DestinationKind::Team => "team",
            DestinationKind::Org => "org",
        }
    }
}

/// Why [`resolve_destination`] refused. Deliberately NOT
/// `connectors::slack_channel_actions::SetDestinationError` — see this
/// module's own doc comment.
#[derive(Debug)]
pub(crate) enum DestinationError {
    /// `ExistingTeam` named a team that does not exist under this account.
    TeamNotFound,
    /// `ExistingTeam` named a real team, but the chooser does not administer
    /// it.
    TeamRequiresAdmin { team_name: String },
    /// `Org` was requested by a non-admin. Contract §5.4's disclosure
    /// requirement ("include the allowed options") is served by the CALLER
    /// (`routes::github`'s handler already has the destinations block it
    /// returned on the preceding GET), not duplicated here.
    OrgRequiresAdmin,
    /// The account has no `kind = 'org'` commons ocean yet — should be
    /// unreachable (every org tenant pool provisions one on first build,
    /// `tenancy::registry::ensure_org_ocean`), reported rather than panicking
    /// if it ever is.
    NoOrgOcean,
    Db(sqlx::Error),
}

impl std::fmt::Display for DestinationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DestinationError::TeamNotFound => write!(f, "team not found"),
            DestinationError::TeamRequiresAdmin { team_name } => {
                write!(f, "'{team_name}' requires a team admin to route a repo into it")
            }
            DestinationError::OrgRequiresAdmin => write!(f, "org_destination_requires_admin"),
            DestinationError::NoOrgOcean => write!(f, "account has no org ocean"),
            DestinationError::Db(e) => write!(f, "database error: {e}"),
        }
    }
}

impl From<sqlx::Error> for DestinationError {
    fn from(e: sqlx::Error) -> Self {
        DestinationError::Db(e)
    }
}

/// Resolve a [`RequestedDestination`] into an authorized [`ResolvedDestination`]
/// — the single choke point every repo-selection write goes through.
///
/// `control_pool` is where `accounts`/`account_members` live (the org-admin
/// check); `tenant_pool` is where `graphs`/`teams`/`team_members` live (the
/// consumer pool for a personal account, or the org's own tenant pool). On
/// the consumer plane every caller passes the same pool for both — the same
/// two-pool convention `connectors::slack`'s own functions use throughout.
pub(crate) async fn resolve_destination(
    control_pool: &PgPool,
    tenant_pool: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
    requested: RequestedDestination,
    repo_owner: &str,
    repo_name: &str,
) -> Result<ResolvedDestination, DestinationError> {
    match requested {
        RequestedDestination::Personal => {
            let plane = account_plane(control_pool, account_id).await?;
            let graph_id =
                resolve_primary_graph(tenant_pool, account_id, plane, IngestCaller::Attributable(user_id)).await?;
            Ok(ResolvedDestination { graph_id, kind: DestinationKind::Personal, newly_created_team: None })
        }
        RequestedDestination::ExistingTeam { team_id } => {
            match check_team_ocean_admin(tenant_pool, account_id, Some(user_id), team_id).await? {
                TeamOceanCheck::Admin { graph_id, .. } => {
                    Ok(ResolvedDestination { graph_id, kind: DestinationKind::Team, newly_created_team: None })
                }
                TeamOceanCheck::NotAdmin { name } => Err(DestinationError::TeamRequiresAdmin { team_name: name }),
                TeamOceanCheck::NotFound => Err(DestinationError::TeamNotFound),
            }
        }
        RequestedDestination::NewTeam => {
            let team_name = format!("{repo_owner}/{repo_name}");
            let (team_id, graph_id) = create_repo_mirrored_team(tenant_pool, account_id, user_id, &team_name).await?;
            Ok(ResolvedDestination { graph_id, kind: DestinationKind::Team, newly_created_team: Some(team_id) })
        }
        RequestedDestination::Org => {
            let proof = crate::tenancy::scope::verify_org_admin(control_pool, account_id, user_id)
                .await
                .map_err(|e| DestinationError::Db(sqlx::Error::Protocol(e.to_string())))?;
            if proof.is_none() {
                return Err(DestinationError::OrgRequiresAdmin);
            }
            let graph_id: Option<Uuid> =
                sqlx::query_scalar("SELECT id FROM graphs WHERE account_id = $1 AND kind = 'org'")
                    .bind(account_id)
                    .fetch_optional(tenant_pool)
                    .await?;
            let graph_id = graph_id.ok_or(DestinationError::NoOrgOcean)?;
            Ok(ResolvedDestination { graph_id, kind: DestinationKind::Org, newly_created_team: None })
        }
    }
}

/// Create a brand-new repo-mirrored team ocean — the GitHub analog of
/// `routes::slack_interactivity::create_org_team_ocean`, duplicated here
/// (that function is private to its own module) rather than reused: same
/// three statements (a `graphs` row, a `teams` row, the creator seated as
/// `'admin'`), one scoped transaction, same idempotent-by-construction shape
/// (a second call always makes a genuinely new team — team names are not
/// unique, matching Slack's own precedent of never de-duplicating on name).
async fn create_repo_mirrored_team(
    pool: &PgPool,
    account_id: Uuid,
    creator_user_id: Uuid,
    name: &str,
) -> Result<(Uuid, Uuid), sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("create_repo_mirrored_team scoped tx: {e}")))?;

    let graph_id: Uuid =
        sqlx::query_scalar("INSERT INTO graphs (account_id, name, kind) VALUES ($1, $2, 'team') RETURNING id")
            .bind(account_id)
            .bind(name)
            .fetch_one(&mut *tx)
            .await?;

    let team_id: Uuid = sqlx::query_scalar(
        "INSERT INTO teams (account_id, graph_id, name, created_by) VALUES ($1, $2, $3, $4) RETURNING id",
    )
    .bind(account_id)
    .bind(graph_id)
    .bind(name)
    .bind(creator_user_id)
    .fetch_one(&mut *tx)
    .await?;

    sqlx::query("INSERT INTO team_members (team_id, user_id, role) VALUES ($1, $2, 'admin')")
        .bind(team_id)
        .bind(creator_user_id)
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;
    Ok((team_id, graph_id))
}

/// The outcome of [`seat_repo_collaborators`] — never a silent guess (C5:
/// "surface 'N authors unmapped' to an admin. Never silently drop unmapped
/// people, and never guess from display names").
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SeatOutcome {
    pub(crate) seated: usize,
    pub(crate) unmapped: usize,
}

/// Auto-seat a repo's mapped collaborators onto a NEWLY-CREATED team ocean
/// (I4) — called once, right after [`create_repo_mirrored_team`] succeeds
/// for a [`RequestedDestination::NewTeam`] pick. Routing to an EXISTING team
/// does not re-run this (§3.4: the admin already administers that team and
/// its membership is theirs to manage).
///
/// Mapping (C5): a collaborator's GitHub `login` is resolved to their public
/// profile email (`GET /users/{login}` — GitHub only returns this when the
/// user has made it public), matched EXACTLY against `users.email` for a
/// user who is a CURRENT member of `account_id`. No match — no public
/// email, or no `users.email` row, or a match that is not a current member
/// — counts as unmapped. Never seated, never guessed from `login`/display
/// name (contract's explicit C5 rule).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn seat_repo_collaborators(
    http: &reqwest::Client,
    control_pool: &PgPool,
    tenant_pool: &PgPool,
    account_id: Uuid,
    team_id: Uuid,
    installation_id: i64,
    repo_id: i64,
    repo_owner: &str,
    repo_name: &str,
) -> Result<SeatOutcome, GithubApiError> {
    let token = super::github::installation_token(http, installation_id).await.map_err(|e| {
        GithubApiError::Transport(e.to_string())
    })?;

    let url = format!(
        "{}/repos/{repo_owner}/{repo_name}/collaborators?per_page=100",
        super::github::github_api_base()
    );
    let collaborators = super::github::get_paginated(http, &token, &url, 20).await?;

    let mut outcome = SeatOutcome::default();
    let source = github_repo_source(repo_id);

    for collaborator in &collaborators {
        let Some(login) = collaborator.get("login").and_then(serde_json::Value::as_str) else {
            outcome.unmapped += 1;
            continue;
        };

        let email = match fetch_public_email(http, &token, login).await {
            Ok(email) => email,
            Err(e) => return Err(e),
        };
        let Some(email) = email.filter(|e| !e.is_empty()) else {
            outcome.unmapped += 1;
            continue;
        };

        let mapped_user_id: Option<Uuid> = match sqlx::query_scalar(
            "SELECT u.id FROM users u \
             JOIN account_members am ON am.user_id = u.id \
             WHERE am.account_id = $1 AND u.email = $2 \
             LIMIT 1",
        )
        .bind(account_id)
        .bind(&email)
        .fetch_optional(control_pool)
        .await
        {
            Ok(id) => id,
            Err(e) => {
                tracing::error!(error = %e, repo_id, "github seating: member lookup failed — counting as unmapped");
                None
            }
        };

        match mapped_user_id {
            Some(user_id) => {
                match record_seat_grant(tenant_pool, account_id, team_id, user_id, &source, None).await {
                    Ok(_) => outcome.seated += 1,
                    Err(e) => {
                        tracing::error!(error = %e, repo_id, "github seating: record_seat_grant failed");
                        outcome.unmapped += 1;
                    }
                }
            }
            None => outcome.unmapped += 1,
        }
    }

    Ok(outcome)
}

/// `Ok(None)` = GitHub answered, no public email. `Err(_)` = the lookup
/// itself failed (rate limit, transport) — never an "unmapped" verdict.
async fn fetch_public_email(
    http: &reqwest::Client,
    token: &str,
    login: &str,
) -> Result<Option<String>, GithubApiError> {
    let url = format!("{}/users/{login}", super::github::github_api_base());
    let body = super::github::get_one(http, token, &url).await?;
    Ok(body.get("email").and_then(serde_json::Value::as_str).map(str::to_string))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn destination_kind_strings() {
        assert_eq!(DestinationKind::Personal.as_str(), "personal");
        assert_eq!(DestinationKind::Team.as_str(), "team");
        assert_eq!(DestinationKind::Org.as_str(), "org");
    }

    #[test]
    fn org_requires_admin_display_matches_the_documented_error_code() {
        assert_eq!(DestinationError::OrgRequiresAdmin.to_string(), "org_destination_requires_admin");
    }

    #[test]
    fn seat_outcome_default_is_zero() {
        let o = SeatOutcome::default();
        assert_eq!(o.seated, 0);
        assert_eq!(o.unmapped, 0);
    }
}

/// pg-gated integration tests — the destination gate against a REAL tenant
/// database (conformance test 4's reachable-without-ingest-workers parts:
/// non-admin picking org is refused; team requires team admin; personal is
/// always allowed).
///
///   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
///     cargo test -p coralswarm-backend --lib github_repos_destination_gate_pg_tests
#[cfg(test)]
mod github_repos_destination_gate_pg_tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection};

    fn base_url() -> Option<String> {
        std::env::var("ORGT_TEST_BASE_URL").ok()
    }
    fn with_db(base: &str, db: &str) -> String {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{db}{params}")
    }
    async fn recreate(base: &str, db: &str) -> PgConnection {
        let mut conn = PgConnection::connect(base).await.expect("connect admin");
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.unwrap();
        conn.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
        conn
    }
    async fn drop_db(admin_conn: &mut PgConnection, db: &str) {
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// A single combined control+tenant pool (Personal-tenancy shape — see
    /// `connectors::google_disconnect`'s own pg tests for the same
    /// single-DB-serves-both-planes precedent), seeded with an `accounts`
    /// row (control-shaped) and a tenant baseline so BOTH the org-admin
    /// check (`accounts`/`account_members`) and the graph/team writes
    /// (`graphs`/`teams`/`team_members`) resolve against the SAME database.
    async fn build_combined_pool(base: &str, db: &str) -> sqlx::PgPool {
        let pool: sqlx::PgPool = PgPoolOptions::new().connect(&with_db(base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
        pool
    }

    async fn seed_account(pool: &sqlx::PgPool) -> Uuid {
        let account_id = Uuid::new_v4();
        let owner_user_id = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(owner_user_id)
            .bind(format!("{account_id}@example.com"))
            .execute(pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO accounts (id, owner_user_id, clerk_org_id) VALUES ($1, $2, 'org_gh_dest_gate')",
        )
        .bind(account_id)
        .bind(owner_user_id)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO account_members (account_id, user_id, role, seat_tier_id) VALUES ($1, $2, 'member', (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))")
            .bind(account_id)
            .bind(owner_user_id)
            .execute(pool)
            .await
            .unwrap();
        account_id
    }

    /// Org destination: a non-admin member is refused
    /// `OrgRequiresAdmin`; an admin succeeds and resolves the account's
    /// `kind='org'` commons ocean.
    #[tokio::test]
    async fn org_destination_requires_org_admin_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping org_destination_requires_org_admin_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_gh_dest_gate_org";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool = build_combined_pool(&base, db).await;

        let account_id = seed_account(&pool).await;
        let org_graph_id: Uuid =
            sqlx::query_scalar("INSERT INTO graphs (account_id, name, kind) VALUES ($1, 'Organization', 'org') RETURNING id")
                .bind(account_id)
                .fetch_one(&pool)
                .await
                .unwrap();

        // Non-admin member.
        let member_id = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(member_id)
            .bind("member@example.com")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO account_members (account_id, user_id, role, seat_tier_id) VALUES ($1, $2, 'member', (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))")
            .bind(account_id)
            .bind(member_id)
            .execute(&pool)
            .await
            .unwrap();

        let refused = resolve_destination(
            &pool,
            &pool,
            account_id,
            member_id,
            RequestedDestination::Org,
            "acme",
            "repo",
        )
        .await;
        assert!(matches!(refused, Err(DestinationError::OrgRequiresAdmin)));

        // Admin member.
        let admin_id = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(admin_id)
            .bind("admin@example.com")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO account_members (account_id, user_id, role, seat_tier_id) VALUES ($1, $2, 'admin', (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))")
            .bind(account_id)
            .bind(admin_id)
            .execute(&pool)
            .await
            .unwrap();

        let ok = resolve_destination(&pool, &pool, account_id, admin_id, RequestedDestination::Org, "acme", "repo")
            .await
            .expect("admin must be allowed to pick the org destination");
        assert_eq!(ok.graph_id, org_graph_id);
        assert_eq!(ok.kind, DestinationKind::Org);

        pool.close().await;
        drop_db(&mut admin_conn, db).await;
    }

    /// Team destination: an existing team requires the chooser to
    /// administer it; a non-admin is refused with the team's name.
    #[tokio::test]
    async fn existing_team_destination_requires_team_admin_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping existing_team_destination_requires_team_admin_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_gh_dest_gate_team";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool = build_combined_pool(&base, db).await;

        let account_id = seed_account(&pool).await;
        let team_admin = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(team_admin)
            .bind("teamadmin@example.com")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO account_members (account_id, user_id, role, seat_tier_id) VALUES ($1, $2, 'member', (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))")
            .bind(account_id)
            .bind(team_admin)
            .execute(&pool)
            .await
            .unwrap();

        let (team_id, _graph_id) =
            create_repo_mirrored_team(&pool, account_id, team_admin, "acme/repo").await.unwrap();

        // The creator (team admin) may route into it.
        let ok = resolve_destination(
            &pool,
            &pool,
            account_id,
            team_admin,
            RequestedDestination::ExistingTeam { team_id },
            "acme",
            "repo",
        )
        .await
        .expect("the team's own admin must be allowed");
        assert_eq!(ok.kind, DestinationKind::Team);

        // A different member, not on the team at all, is refused.
        let other = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(other)
            .bind("other@example.com")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO account_members (account_id, user_id, role, seat_tier_id) VALUES ($1, $2, 'member', (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))")
            .bind(account_id)
            .bind(other)
            .execute(&pool)
            .await
            .unwrap();

        let refused = resolve_destination(
            &pool,
            &pool,
            account_id,
            other,
            RequestedDestination::ExistingTeam { team_id },
            "acme",
            "repo",
        )
        .await;
        assert!(matches!(refused, Err(DestinationError::TeamRequiresAdmin { .. })));

        pool.close().await;
        drop_db(&mut admin_conn, db).await;
    }

    /// Personal destination is always allowed, for any account member —
    /// resolves to their own primary ocean.
    #[tokio::test]
    async fn personal_destination_is_always_allowed_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping personal_destination_is_always_allowed_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_gh_dest_gate_personal";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool = build_combined_pool(&base, db).await;

        let account_id = seed_account(&pool).await;
        let member = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(member)
            .bind("solo@example.com")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO account_members (account_id, user_id, role, seat_tier_id) VALUES ($1, $2, 'member', (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))")
            .bind(account_id)
            .bind(member)
            .execute(&pool)
            .await
            .unwrap();
        // `resolve_primary_graph`'s Org branch refuses to auto-create a
        // member ocean (MEMBER_OCEANS_ENABLED off in this test process) —
        // seed the primary member ocean directly rather than pull in that
        // flag/provisioning machinery, which is orthogonal to what this
        // test proves (the destination GATE, not ocean auto-provisioning).
        sqlx::query(
            "INSERT INTO graphs (account_id, name, kind, owner_user_id, is_primary) \
             VALUES ($1, 'Member ocean', 'member', $2, true)",
        )
        .bind(account_id)
        .bind(member)
        .execute(&pool)
        .await
        .unwrap();

        let ok = resolve_destination(&pool, &pool, account_id, member, RequestedDestination::Personal, "acme", "repo")
            .await
            .expect("personal destination must always be allowed");
        assert_eq!(ok.kind, DestinationKind::Personal);

        pool.close().await;
        drop_db(&mut admin_conn, db).await;
    }
}
