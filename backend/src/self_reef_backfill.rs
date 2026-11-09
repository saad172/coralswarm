//! Reusable core of the `backfill_self_reefs` binary
//! (`docs/plans/identity-resolution-at-the-door.md` §10.3, builder B1, and
//! its follow-up gap: a graph that already HAS a self reef — e.g. the
//! primary ocean `93cc446a`, whose self reef "Alex Example" was created by
//! Slack ingest before identifiers existed — never gets `clerk_user`/
//! `email` rows in `reef_identifiers`, because the original selection was
//! `self_node_id IS NULL` only). `backend/src/bin/backfill_self_reefs.rs` is
//! a thin argument-parse + DSN-resolution wrapper around
//! [`list_eligible_graphs`]/[`process_graph`]; every gate, counter and write
//! lives HERE so `backend/tests/identity_door_pg.rs` drives the same code
//! an operator runs, not a re-implementation of it.
//!
//! # Two graph cases, one pass
//!
//! * [`GraphCase::NoSelfReef`] — the original target: `self_node_id IS
//!   NULL`. Creates the self reef (title resolution, `needs_name` stamp)
//!   and binds identifiers. Unchanged from the original binary.
//! * [`GraphCase::ExistingSelfReef`] — the new target: `self_node_id IS NOT
//!   NULL` but the self reef has no `reef_identifiers` row with
//!   `kind='clerk_user'`. Never creates or retitles anything — only binds
//!   identifiers onto the self reef that already exists, via the same
//!   [`bind_principal_identifiers_values`] helper the create path uses.
//!
//! # Email collision policy
//!
//! `reef_identifiers` is `UNIQUE (graph_id, kind, value_norm)`. When the
//! owner's verified email is already held by a DIFFERENT node in the same
//! graph — an "owner fragment" reef born before this door existed — binding
//! it onto the self reef would either silently no-op (`ON CONFLICT DO
//! NOTHING`, the value stays on the fragment) or would need to steal it,
//! which this backfill never does: identifier reassignment is a merge
//! decision, not a backfill's to make. So the INSERT still goes through
//! `ON CONFLICT DO NOTHING` (belt and suspenders — never a hard failure),
//! but [`detect_email_collision`] checks first and the report names the
//! fragment (node id, title, coral/edge counts) and points at the
//! `merge_reefs` operator bin as the fix. `clerk_user` never collides in
//! practice (nothing else can legitimately hold a Clerk user id), so it
//! always binds.
//!
//! # Idempotent
//!
//! A second run selects zero graphs: [`list_eligible_graphs`] excludes any
//! graph whose self reef already carries `kind='clerk_user'`, and every
//! identifier write is `ON CONFLICT DO NOTHING`.

use sqlx::PgPool;

/// Refuse to run under a role that row-level security would silently filter.
///
/// Both operator bins built on this module (`backfill_self_reefs`,
/// `merge_reefs`) walk graphs ACROSS accounts by design, so
/// `tenancy::scope::begin_account_scoped` is the wrong shape and their
/// direct SQL runs PLAIN. Under the least-privilege `coralswarm_app` /
/// `coralswarm_tenant_app` role that does not error — the `nodes` policy just
/// hides every row and the tool reports "nothing to do". This is the same
/// preflight-not-trust check `judgement_backfill::assert_policy_bypass`
/// performs, checked against
/// `nodes` (the table every query here touches). Returns the operator-facing
/// refusal text as the error.
pub async fn assert_policy_bypass(pool: &PgPool) -> Result<(), String> {
    let table: Option<String> = sqlx::query_scalar("SELECT to_regclass('nodes')::text")
        .fetch_one(pool)
        .await
        .map_err(|e| format!("preflight query failed: {e}"))?;
    if table.is_none() {
        return Err("no `nodes` table in this database — migrate_control / migrate_tenants has not \
                    been applied here. Run the matching migrator first."
            .to_string());
    }
    let (bypasses, is_owner, forced): (bool, bool, bool) = sqlx::query_as(
        "SELECT \
            (SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = current_user), \
            c.relowner = (SELECT oid FROM pg_roles WHERE rolname = current_user), \
            c.relforcerowsecurity \
         FROM pg_class c WHERE c.oid = 'nodes'::regclass",
    )
    .fetch_one(pool)
    .await
    .map_err(|e| format!("preflight query failed: {e}"))?;
    if bypasses || (is_owner && !forced) {
        return Ok(());
    }
    Err(format!(
        "REFUSING: this connection's role does not bypass row-level security on `nodes` \
         (superuser/BYPASSRLS = {bypasses}, owns the table = {is_owner}, FORCE ROW LEVEL \
         SECURITY = {forced}). This tool walks every graph in the database by design; under \
         the least-privilege coralswarm_app/coralswarm_tenant_app role the RLS policy would \
         hide every row and the run would report nothing to do. Point MIGRATOR_DATABASE_URL \
         (control plane) or the tenant owner credential (tenant plane) at the ELEVATED role \
         and re-run."
    ))
}
use uuid::Uuid;

use crate::store::{
    bind_principal_identifiers_values, door_stamp_needs_name, resolve_principal_title_scoped,
    PostgresStore, Store,
};

/// Which of the two backfill targets a selected graph is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphCase {
    /// `graphs.self_node_id IS NULL` — the original target. A self reef is
    /// created and identifiers bound to it.
    NoSelfReef,
    /// `graphs.self_node_id IS NOT NULL` but the self reef has no
    /// `clerk_user` identifier. Bind-only — never created or retitled.
    ExistingSelfReef { self_node_id: Uuid },
}

/// One graph selected by [`list_eligible_graphs`].
#[derive(Debug, Clone)]
pub struct EligibleGraph {
    pub id: Uuid,
    pub account_id: Uuid,
    pub owner_user_id: Uuid,
    pub case: GraphCase,
}

/// A DIFFERENT node than the self reef already holding the owner's email
/// identifier — an owner fragment this backfill will not move or delete.
#[derive(Debug, Clone)]
pub struct EmailCollision {
    pub node_id: Uuid,
    pub title: String,
    pub coral_count: i64,
    pub degree: i64,
    /// The normalized email value that collided (echoed here so the report
    /// line doesn't need a second parameter threaded through).
    pub value_norm: String,
}

impl EmailCollision {
    /// Exact wording the design doc / task calls for: names the fragment
    /// and points at the sibling merge operator bin.
    pub fn report_line(&self) -> String {
        format!(
            "email identifier {} is held by node {} \"{}\" ({} corals, {} edges) — that reef is \
             an owner fragment; merge it into the self reef with merge_reefs",
            self.value_norm, self.node_id, self.title, self.coral_count, self.degree
        )
    }
}

/// Per-graph outcome, common to dry-run and `--execute` — `GraphReport`'s
/// `execute` flag alone decides whether the [`std::fmt::Display`] impl says
/// "would_bind"/"would create" or "BOUND"/"CREATED"; the underlying facts
/// (what WOULD happen) are identical in both modes, which is what makes a
/// dry run's prediction exact.
#[derive(Debug, Clone)]
pub enum Outcome {
    /// No `public.users` table reachable on this connection (a pure tenant
    /// database) — nothing to look up, nothing done for this graph.
    NoUsersTable,
    /// [`GraphCase::NoSelfReef`] path.
    Created { title: String, needs_name: bool, clerk_user: bool, email: bool },
    /// [`GraphCase::ExistingSelfReef`] path.
    ExistingSelfReef {
        self_node_id: Uuid,
        clerk_user: bool,
        email: bool,
        email_collision: Option<EmailCollision>,
    },
    Error(String),
}

#[derive(Debug, Clone)]
pub struct GraphReport {
    pub graph_id: Uuid,
    pub account_id: Uuid,
    pub owner_user_id: Uuid,
    pub execute: bool,
    pub outcome: Outcome,
}

impl std::fmt::Display for GraphReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let verb = if self.execute { "BOUND" } else { "would_bind" };
        match &self.outcome {
            Outcome::NoUsersTable => write!(
                f,
                "graph={} account={} owner={} no users table on this connection — skipped \
                 (see module doc)",
                self.graph_id, self.account_id, self.owner_user_id
            ),
            Outcome::Error(e) => write!(
                f,
                "graph={} account={} owner={} ERROR: {e}",
                self.graph_id, self.account_id, self.owner_user_id
            ),
            Outcome::Created { title, needs_name, clerk_user, email } => write!(
                f,
                "graph={} account={} owner={} {verb} title={title:?} needs_name={needs_name} \
                 clerk_user={clerk_user} email={email}",
                self.graph_id, self.account_id, self.owner_user_id
            ),
            Outcome::ExistingSelfReef { self_node_id, clerk_user, email, email_collision } => {
                write!(
                    f,
                    "graph={} account={} owner={} EXISTING self_node={self_node_id} {verb}: \
                     clerk_user={clerk_user} email={email}",
                    self.graph_id, self.account_id, self.owner_user_id
                )?;
                if let Some(c) = email_collision {
                    write!(f, "\n{}", c.report_line())?;
                }
                Ok(())
            }
        }
    }
}

/// Running counts across a run, printed as one machine-greppable summary
/// line — the `migrate_tenants`/`judgement_backfill` convention.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Totals {
    pub seen: u32,
    /// [`GraphCase::NoSelfReef`] graphs actually created/bound. Only
    /// incremented on a committed `--execute` write — a dry run always
    /// reports `0` here (read the per-graph `would_bind` lines instead),
    /// matching the original binary's counting convention.
    pub created: u32,
    /// [`GraphCase::ExistingSelfReef`] graphs where `clerk_user` was
    /// actually bound. Same `--execute`-only counting as `created`.
    pub existing_bound: u32,
    /// Email collisions detected — see [`EmailCollision`]. Counted whether
    /// or not `--execute` was passed, since the collision exists in the
    /// data either way.
    pub email_collisions: u32,
    pub no_users_table: u32,
    pub errors: u32,
}

impl std::fmt::Display for Totals {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "summary: graphs={} created={} existing_bound={} email_collisions={} \
             no_users_table={} errors={}",
            self.seen, self.created, self.existing_bound, self.email_collisions, self.no_users_table,
            self.errors
        )
    }
}

/// `to_regclass('public.users')` — `true` iff this connection's database has
/// a `users` table (the consumer/control plane; false on a pure org tenant
/// database, see the binary's module doc).
pub async fn users_table_present(pool: &PgPool) -> bool {
    sqlx::query_scalar::<_, Option<String>>("SELECT to_regclass('public.users')::text")
        .fetch_one(pool)
        .await
        .ok()
        .flatten()
        .is_some()
}

/// Selection: live (`archived_at IS NULL`), `kind IN ('personal',
/// 'member')` graphs that are EITHER self-reef-less
/// ([`GraphCase::NoSelfReef`]) OR already have a self reef that is missing
/// a `clerk_user` identifier ([`GraphCase::ExistingSelfReef`]). `kind =
/// 'org'` is excluded for the same reason the original selection excluded
/// it — see the binary's module doc.
pub async fn list_eligible_graphs(
    pool: &PgPool,
    only_graph: Option<Uuid>,
) -> Result<Vec<EligibleGraph>, sqlx::Error> {
    let rows: Vec<(Uuid, Uuid, Option<Uuid>, Uuid, Option<Uuid>)> = sqlx::query_as(
        "SELECT g.id, g.account_id, g.owner_user_id, acc.owner_user_id AS account_owner, \
                g.self_node_id \
         FROM graphs g \
         JOIN accounts acc ON acc.id = g.account_id \
         WHERE g.archived_at IS NULL \
           AND g.kind IN ('personal', 'member') \
           AND ($1::uuid IS NULL OR g.id = $1) \
           AND ( \
             g.self_node_id IS NULL \
             OR NOT EXISTS ( \
                 SELECT 1 FROM reef_identifiers ri \
                 WHERE ri.graph_id = g.id AND ri.node_id = g.self_node_id \
                   AND ri.kind = 'clerk_user' \
             ) \
           ) \
         ORDER BY g.created_at ASC",
    )
    .bind(only_graph)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(id, account_id, owner, account_owner, self_node_id)| {
            let case = match self_node_id {
                Some(n) => GraphCase::ExistingSelfReef { self_node_id: n },
                None => GraphCase::NoSelfReef,
            };
            EligibleGraph { id, account_id, owner_user_id: owner.unwrap_or(account_owner), case }
        })
        .collect())
}

/// Is `email_norm` (already lowercased/trimmed) held by a node OTHER than
/// `self_node_id` in `graph_id`? If so, that node is an owner fragment —
/// see the module doc's collision policy.
pub async fn detect_email_collision(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    graph_id: Uuid,
    self_node_id: Uuid,
    email_norm: &str,
) -> Result<Option<EmailCollision>, sqlx::Error> {
    if email_norm.is_empty() {
        return Ok(None);
    }
    let row: Option<(Uuid, String, i64, i64)> = sqlx::query_as(
        "SELECT n.node_id, n.title, n.coral_count::bigint AS coral_count, \
                COALESCE((SELECT count(*) FROM relationship_edge e \
                          WHERE e.src_node_id = n.node_id OR e.dst_node_id = n.node_id), 0) \
                    AS degree \
         FROM reef_identifiers ri \
         JOIN nodes n ON n.node_id = ri.node_id \
         WHERE ri.graph_id = $1 AND ri.kind = 'email' AND ri.value_norm = $2 \
           AND ri.node_id <> $3",
    )
    .bind(graph_id)
    .bind(email_norm)
    .bind(self_node_id)
    .fetch_optional(&mut **tx)
    .await?;

    Ok(row.map(|(node_id, title, coral_count, degree)| EmailCollision {
        node_id,
        title,
        coral_count,
        degree,
        value_norm: email_norm.to_string(),
    }))
}

/// Process one selected graph: dry-run (`execute=false`) performs every
/// lookup and check but writes nothing (the transaction is rolled back);
/// `execute=true` applies it. Mutates `totals` either way — a dry run's
/// counters predict an `--execute` run exactly, which is the whole point of
/// a report an operator authorises a real run by reading.
pub async fn process_graph(
    pool: &PgPool,
    graph: &EligibleGraph,
    users_present: bool,
    execute: bool,
    totals: &mut Totals,
) -> GraphReport {
    totals.seen += 1;

    let mk = |outcome: Outcome| GraphReport {
        graph_id: graph.id,
        account_id: graph.account_id,
        owner_user_id: graph.owner_user_id,
        execute,
        outcome,
    };

    if !users_present {
        totals.no_users_table += 1;
    }

    let user_row: Option<(Option<String>, String, Option<String>)> = if users_present {
        match sqlx::query_as("SELECT name, email, clerk_user_id FROM users WHERE id = $1")
            .bind(graph.owner_user_id)
            .fetch_optional(pool)
            .await
        {
            Ok(row) => row,
            Err(e) => {
                totals.errors += 1;
                return mk(Outcome::Error(format!("users lookup: {e}")));
            }
        }
    } else {
        None
    };

    let mut tx = match pool.begin().await {
        Ok(tx) => tx,
        Err(e) => {
            totals.errors += 1;
            return mk(Outcome::Error(format!("tx begin: {e}")));
        }
    };

    let outcome: Result<Outcome, String> = async {
        match graph.case {
            GraphCase::NoSelfReef => {
                let (title, needs_name, clerk_user_id, email) = match &user_row {
                    Some((name, email, clerk_user_id)) => {
                        let (title, needs_name) =
                            resolve_principal_title_scoped(&mut tx, graph.id, name.as_deref(), email)
                                .await;
                        (title, needs_name, clerk_user_id.clone(), Some(email.clone()))
                    }
                    None => ("Owner".to_string(), true, None, None),
                };

                if execute {
                    let store = PostgresStore::new(pool.clone());
                    let (node_id, _self_title) = store
                        .ensure_self_node_scoped(&mut tx, graph.id, graph.owner_user_id, &title)
                        .await
                        .map_err(|e| e.to_string())?;
                    if needs_name {
                        door_stamp_needs_name(&mut tx, node_id).await.map_err(|e| e.to_string())?;
                    }
                    bind_principal_identifiers_values(
                        &mut tx,
                        graph.id,
                        node_id,
                        clerk_user_id.as_deref(),
                        email.as_deref(),
                    )
                    .await
                    .map_err(|e| e.to_string())?;
                }

                Ok(Outcome::Created {
                    title,
                    needs_name,
                    clerk_user: clerk_user_id.is_some(),
                    email: email.is_some(),
                })
            }
            GraphCase::ExistingSelfReef { self_node_id } => {
                let clerk_user_id = user_row.as_ref().and_then(|(_, _, c)| c.clone());
                let email = user_row.as_ref().map(|(_, e, _)| e.clone());
                let email_norm = email.as_deref().map(|e| e.trim().to_lowercase());

                let collision = match email_norm.as_deref().filter(|e| !e.is_empty()) {
                    Some(e) => detect_email_collision(&mut tx, graph.id, self_node_id, e)
                        .await
                        .map_err(|e| e.to_string())?,
                    None => None,
                };

                if execute {
                    bind_principal_identifiers_values(
                        &mut tx,
                        graph.id,
                        self_node_id,
                        clerk_user_id.as_deref(),
                        email.as_deref(),
                    )
                    .await
                    .map_err(|e| e.to_string())?;
                }

                Ok(Outcome::ExistingSelfReef {
                    self_node_id,
                    clerk_user: clerk_user_id.is_some(),
                    email: email.is_some() && collision.is_none(),
                    email_collision: collision,
                })
            }
        }
    }
    .await;

    match outcome {
        Ok(o) => {
            // Collisions are a fact about the data, true in both modes — a
            // dry run must surface them, not just an `--execute` run.
            if let Outcome::ExistingSelfReef { email_collision: Some(_), .. } = &o {
                totals.email_collisions += 1;
            }
            if execute {
                match tx.commit().await {
                    Ok(()) => match &o {
                        Outcome::Created { .. } => totals.created += 1,
                        Outcome::ExistingSelfReef { clerk_user: true, .. } => {
                            totals.existing_bound += 1
                        }
                        _ => {}
                    },
                    Err(e) => {
                        totals.errors += 1;
                        return mk(Outcome::Error(format!("commit: {e}")));
                    }
                }
            } else {
                tx.rollback().await.ok();
            }
            mk(o)
        }
        Err(e) => {
            tx.rollback().await.ok();
            totals.errors += 1;
            mk(Outcome::Error(e))
        }
    }
}
