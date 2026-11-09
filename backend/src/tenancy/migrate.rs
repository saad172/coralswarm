//! Tenant-tree migration orchestration (org-tenancy PR 3.1).
//!
//! The per-org TENANT tree is applied out-of-band by this orchestrator (the
//! `migrate_tenants` binary), never at web-server boot — so a slow or failed
//! tenant never blocks a deploy. The CONTROL tree is applied the same way, by
//! `migrate_control` (the Fly deploy `release_command`); boot applies neither.
//!
//! Per tenant: take a Postgres advisory lock (so two orchestrators never migrate
//! the same DB at once), apply the embedded tenant tree (which also validates
//! existing checksums), verify the DB's `tenant_meta` fingerprint if present,
//! and record the applied version back onto the routing row. Per-tenant failures
//! are isolated — one bad tenant is reported and skipped, never aborting the run.
//! `--check` reports drift without mutating.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::stream::{self, StreamExt};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{PgConnection, PgPool};

use super::routing::{self, TenantDatabase};
use super::secrets::SecretResolver;

/// The embedded tenant migration tree.
pub static TENANT_MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations_tenant");

/// Advisory-lock key serializing migration of a single tenant DB (arbitrary,
/// stable constant). Taken on the tenant connection, so it scopes per tenant DB.
const ADVISORY_LOCK_KEY: i64 = 0x636F_7261_6C6D_6967; // "coralmig"

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Apply pending tenant migrations.
    Migrate,
    /// Report drift (pending / checksum mismatch / unknown-applied) without
    /// mutating.
    Check,
}

#[derive(Debug, Clone)]
pub struct MigrateOpts {
    pub mode: Mode,
    /// Restrict to a single tenant by slug or clerk_org_id.
    pub only: Option<String>,
    /// Bounded parallelism across tenants (each is an independent DB with its
    /// own advisory lock, so concurrency is safe).
    pub jobs: usize,
    pub connect_timeout: Duration,
    /// How long one DDL statement may queue for a table lock, and how the
    /// run retries after giving up. See [`DdlLockPolicy`].
    pub ddl_lock: DdlLockPolicy,
    /// Set only by the self-serve provisioning job's in-process run (on its
    /// `coralswarm_provisioner` pool): record each applied version through
    /// `tenant_provisioning_record_schema_version` under this lease instead
    /// of the release's owner-role UPDATE. `None` (the release, every
    /// operator run) records with the direct UPDATE only.
    pub provisioning_lease: Option<ProvisioningLease>,
}

/// The provisioning job's lease, for [`MigrateOpts::provisioning_lease`].
#[derive(Debug, Clone)]
pub struct ProvisioningLease {
    pub owner: String,
    pub ttl: Duration,
}

impl Default for MigrateOpts {
    fn default() -> Self {
        MigrateOpts {
            mode: Mode::Migrate,
            only: None,
            jobs: 4,
            connect_timeout: Duration::from_secs(15),
            ddl_lock: DdlLockPolicy::default(),
            provisioning_lease: None,
        }
    }
}

/// Lock discipline for applying a migration tree to a live database.
///
/// DDL on a hot table (`CREATE TRIGGER` on `source_items` /
/// `source_versions` / `semantic_atoms`, `ALTER TABLE`, …) needs a
/// `SHARE ROW EXCLUSIVE` or stronger lock. PostgreSQL grants locks in
/// queue order, so a migration that queues behind one long-running write
/// transaction makes EVERY later writer on that table queue behind the
/// migration — an ingest stall across the whole plane for as long as that
/// one transaction lasts, caused by a statement that itself takes
/// milliseconds once it runs. `lock_timeout` bounds the wait: after
/// `lock_timeout` the statement fails with `55P03`, its migration's
/// transaction rolls back (sqlx wraps each migration in one) and every
/// queued writer proceeds. The run then sleeps and retries; the ledger of
/// applied migrations makes a re-run idempotent, so only the migration that
/// gave up is attempted again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DdlLockPolicy {
    /// Session `lock_timeout` for the whole run. Short enough that a stall
    /// behind the migration is a blip, long enough for ordinary write
    /// transactions to finish.
    pub lock_timeout: Duration,
    /// Attempts at the tree before the tenant is reported failed.
    pub attempts: u32,
    /// Pause between attempts, letting the contending transaction finish.
    pub retry_backoff: Duration,
}

impl Default for DdlLockPolicy {
    fn default() -> Self {
        DdlLockPolicy {
            lock_timeout: Duration::from_secs(5),
            attempts: 6,
            retry_backoff: Duration::from_secs(2),
        }
    }
}

impl DdlLockPolicy {
    /// PostgreSQL rounds `lock_timeout` to whole milliseconds; zero disables
    /// it, which is never what a caller asking for a bound means.
    fn lock_timeout_ms(&self) -> u64 {
        self.lock_timeout.as_millis().clamp(1, i32::MAX as u128) as u64
    }
}

/// `55P03 lock_not_available`: the only error class a retry can fix.
fn is_lock_timeout(error: &sqlx::migrate::MigrateError) -> bool {
    use sqlx::migrate::MigrateError::{Execute, ExecuteMigration};
    let db = match error {
        Execute(sqlx::Error::Database(db)) | ExecuteMigration(sqlx::Error::Database(db), _) => db,
        _ => return false,
    };
    db.code().as_deref() == Some("55P03")
}

/// Apply `migrator` on `conn` under `policy`. `conn` must be a direct,
/// pinned session (never a transaction pooler): the timeout is a session
/// setting so that it covers every statement of every migration, and the
/// retry re-uses the same session. `label` names the database in logs only.
pub async fn apply_tree(
    migrator: &sqlx::migrate::Migrator,
    conn: &mut PgConnection,
    policy: DdlLockPolicy,
    label: &str,
) -> Result<(), sqlx::migrate::MigrateError> {
    sqlx::query("SELECT set_config('lock_timeout',$1,false)")
        .bind(format!("{}ms", policy.lock_timeout_ms()))
        .execute(&mut *conn)
        .await?;
    let attempts = policy.attempts.max(1);
    let mut attempt = 1;
    loop {
        // `run_direct`, not `run`: `run`'s `Acquire<'a>` bound is "not
        // general enough" for a future that must be `'static + Send` (a
        // spawned migration task); sqlx exposes `run_direct` for that case.
        match migrator.run_direct(&mut *conn).await {
            Ok(()) => return Ok(()),
            Err(error) if is_lock_timeout(&error) && attempt < attempts => {
                tracing::warn!(
                    database = label,
                    attempt,
                    attempts,
                    lock_timeout_ms = policy.lock_timeout_ms(),
                    error = %error,
                    "migration gave up a table lock wait rather than block writers; retrying"
                );
                // sqlx takes its own session advisory lock at the start of
                // `run` and releases it only on the success path. A session
                // may re-take a lock it holds, so the retry would still run,
                // but the lock count would then outlive the run; release it.
                if let Err(unlock) = sqlx::migrate::Migrate::unlock(&mut *conn).await {
                    tracing::warn!(database = label, error = %unlock, "could not release sqlx's migration lock before retrying");
                }
                tokio::time::sleep(policy.retry_backoff).await;
                attempt += 1;
            }
            Err(error) => return Err(error),
        }
    }
}

#[derive(Debug)]
pub enum TenantResult {
    Migrated { applied_to_version: i64 },
    UpToDate { version: i64 },
    Drift { detail: String },
    Skipped { reason: String },
    Failed { error: String },
    /// The tenant database records a tenant migration past this build's head
    /// (sqlx `VersionMissing` with a version above the head): a newer build already migrated it and
    /// this one is older (a rollback, or an older machine still running
    /// mid-deploy). Nothing was applied. Not something this build can fix:
    /// the release counts it as a failure, exactly as before; the
    /// self-serve provisioning job parks on it (never spends an attempt and
    /// never cleans up) until a build at least that new takes the job over.
    AheadOfBuild { version: i64, head: i64 },
}

#[derive(Debug)]
pub struct TenantOutcome {
    pub clerk_org_id: String,
    pub slug: String,
    pub result: TenantResult,
}

#[derive(Debug)]
pub struct MigrateReport {
    pub outcomes: Vec<TenantOutcome>,
}

/// Per-outcome tallies for one run. Exists so callers (the `migrate_tenants`
/// binary, and through it the Fly `release_command`) can print ONE line that
/// distinguishes "migrated everything" from "quietly skipped a tenant" —
/// a deploy that established the schema-version invariant for every tenant
/// must not look identical to one that did not.
///
/// Caveat on reading the tally: in [`Mode::Migrate`], `run_migrate` reports
/// [`TenantResult::Migrated`] whether or not there was anything pending (the
/// sqlx apply is idempotent but does not say how many statements it ran), so
/// `up_to_date` is only ever populated under [`Mode::Check`]. `migrated` in a
/// deploy log therefore means "at head", not "changed". The distinction that
/// matters here — at head vs skipped vs failed — is exact.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct MigrateCounts {
    pub migrated: usize,
    pub up_to_date: usize,
    pub drifted: usize,
    pub skipped: usize,
    pub failed: usize,
}

impl MigrateCounts {
    /// Tenants considered by this run.
    pub fn total(&self) -> usize {
        self.migrated + self.up_to_date + self.drifted + self.skipped + self.failed
    }
}

impl MigrateReport {
    /// A run is a failure if any tenant errored, or (in `--check`) drifted.
    pub fn any_failed(&self) -> bool {
        self.outcomes.iter().any(|o| {
            matches!(
                o.result,
                TenantResult::Failed { .. } | TenantResult::Drift { .. } | TenantResult::AheadOfBuild { .. }
            )
        })
    }

    /// True if any tenant was passed over without its state being established
    /// — today only the non-blocking advisory-lock miss (another orchestrator
    /// holds that tenant's lock).
    ///
    /// Deliberately NOT folded into [`Self::any_failed`]: a skip is not an
    /// error, and the two must stay distinguishable so the caller can report
    /// (and exit) differently. It is nonetheless *not* success either — this
    /// run did not prove `schema_version >= head_version()` for that tenant,
    /// which is precisely the condition the request path 503s on. Callers that
    /// gate a deploy on this run should treat it as non-zero; see
    /// `bin/migrate_tenants.rs`'s exit-code table.
    pub fn any_skipped(&self) -> bool {
        self.outcomes
            .iter()
            .any(|o| matches!(o.result, TenantResult::Skipped { .. }))
    }

    /// Tally the outcomes by kind.
    pub fn counts(&self) -> MigrateCounts {
        let mut c = MigrateCounts::default();
        for o in &self.outcomes {
            match o.result {
                TenantResult::Migrated { .. } => c.migrated += 1,
                TenantResult::UpToDate { .. } => c.up_to_date += 1,
                TenantResult::Drift { .. } => c.drifted += 1,
                TenantResult::Skipped { .. } => c.skipped += 1,
                // Counted as failed: the release could not establish head for
                // it, and its exit code must not change for a rollback.
                TenantResult::Failed { .. } | TenantResult::AheadOfBuild { .. } => c.failed += 1,
            }
        }
        c
    }
}

/// Head (max) version of the embedded tenant tree — the tenant-schema version
/// this binary's queries assume. `pub`: the worker tenant harness compares it
/// against `tenant_databases.schema_version` (its schema-version gate), and
/// tests seed routing rows with it.
pub fn head_version() -> i64 {
    TENANT_MIGRATOR.iter().map(|m| m.version).max().unwrap_or(0)
}

async fn select_tenants(
    control: &PgPool,
    opts: &MigrateOpts,
) -> Result<Vec<TenantDatabase>, sqlx::Error> {
    if let Some(only) = &opts.only {
        // Accept either a slug or a clerk_org_id.
        let row = match routing::lookup_by_slug(control, only).await? {
            Some(r) => Some(r),
            None => routing::lookup_by_org(control, only).await?,
        };
        Ok(row.into_iter().collect())
    } else {
        routing::list_migratable(control).await
    }
}

/// Orchestrate migration/drift-check across the selected tenants.
///
/// A full `Mode::Migrate` run (no `--only`: the release) ends with a
/// catch-up pass. `list_migratable` leaves out a self-serve tenant whose
/// provisioning job is still live; if that job activates the tenant while
/// this run is in flight, the tenant is active, was never selected, and may
/// sit behind this build's head. So the run lists again once every write of
/// the first pass has landed and migrates any newly selectable tenant that is
/// not at head. Anything activated after that re-list is refused by
/// `tenant_provisioning_activate` itself (control 0381): it will not
/// activate a tenant below a version another live routing row already
/// records. Idempotent and per-tenant locked like the first pass.
pub async fn migrate_all(
    control: &PgPool,
    resolver: Arc<dyn SecretResolver>,
    opts: &MigrateOpts,
) -> Result<MigrateReport, sqlx::Error> {
    let tenants = select_tenants(control, opts).await?;
    let mut outcomes = run_tenants(control, &resolver, opts, tenants).await;

    if opts.only.is_none() && opts.mode == Mode::Migrate {
        let seen: HashSet<String> = outcomes.iter().map(|o| o.clerk_org_id.clone()).collect();
        let head = head_version();
        let late: Vec<TenantDatabase> = routing::list_migratable(control)
            .await?
            .into_iter()
            .filter(|t| !seen.contains(&t.clerk_org_id) && t.schema_version.map_or(true, |v| v < head))
            .collect();
        if !late.is_empty() {
            tracing::info!(count = late.len(), "tenant(s) became migratable during this run; catching up");
            outcomes.extend(run_tenants(control, &resolver, opts, late).await);
        }
    }

    Ok(MigrateReport { outcomes })
}

async fn run_tenants(
    control: &PgPool,
    resolver: &Arc<dyn SecretResolver>,
    opts: &MigrateOpts,
    tenants: Vec<TenantDatabase>,
) -> Vec<TenantOutcome> {
    let control = control.clone();
    let mode = opts.mode;
    let connect_timeout = opts.connect_timeout;
    let ddl_lock = opts.ddl_lock;
    let lease = opts.provisioning_lease.clone();

    // Every per-tenant future is built from OWNED clones and boxed to
    // `dyn Future + Send` before it enters the stream. With the closure
    // borrowing `resolver`/`control` from this frame, rustc could not prove
    // the whole `migrate_all` future `Send` for every lifetime ("not general
    // enough") once a spawned task — the self-serve provisioning worker —
    // awaited it. Behaviour is identical; only the type is simpler.
    let jobs = opts.jobs.max(1);
    let futures: Vec<Pin<Box<dyn Future<Output = TenantOutcome> + Send>>> = tenants
        .into_iter()
        .map(|t| {
            let resolver = resolver.clone();
            let control = control.clone();
            let lease = lease.clone();
            Box::pin(async move { process_one(&control, resolver, t, mode, connect_timeout, ddl_lock, lease).await })
                as Pin<Box<dyn Future<Output = TenantOutcome> + Send>>
        })
        .collect();
    stream::iter(futures)
        .buffer_unordered(jobs)
        .collect::<Vec<_>>()
        .await
}

async fn process_one(
    control: &PgPool,
    resolver: Arc<dyn SecretResolver>,
    t: TenantDatabase,
    mode: Mode,
    connect_timeout: Duration,
    ddl_lock: DdlLockPolicy,
    lease: Option<ProvisioningLease>,
) -> TenantOutcome {
    // Owned, `move` closure rather than one borrowing `t` across the awaits
    // below: a borrowing closure held across an await point makes this
    // future's `Send` obligation higher-ranked, which rustc cannot prove
    // when the orchestrator runs inside a spawned task (the self-serve
    // provisioning worker does exactly that).
    let (mk_org, mk_slug) = (t.clerk_org_id.clone(), t.slug.clone());
    let mk = move |result| TenantOutcome {
        clerk_org_id: mk_org.clone(),
        slug: mk_slug.clone(),
        result,
    };

    let secret_ref = t.db_secret_ref.clone();
    let dsn = match resolver.resolve_async(&secret_ref).await {
        Ok(d) => d,
        Err(e) => {
            return mk(TenantResult::Failed {
                error: format!("secret: {e}"),
            })
        }
    };
    // Session advisory locks require a direct server session. A pinned SQLx
    // connection alone does not prevent transaction poolers changing backends.
    let connect_options = match migration_connect_options(&dsn) {
        Ok(options) => options,
        Err(_) => {
            return mk(TenantResult::Failed {
                error: "invalid tenant connection options".into(),
            })
        }
    };
    let pool = match PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(connect_timeout)
        .connect_with(connect_options)
        .await
    {
        Ok(p) => p,
        Err(e) => {
            return mk(TenantResult::Failed {
                error: format!("connect: {e}"),
            })
        }
    };

    let result = process_locked(&pool, &t, mode, ddl_lock).await;

    // On a successful apply, record the version onto the control routing row.
    // A write that does not land is a FAILED outcome, not a warning: the
    // worker harness treats a NULL/behind `schema_version` as "skip this
    // tenant forever" and the request path 503s it, so reporting `Migrated`
    // here with nothing recorded is an absence that reads as success (audit
    // 4d). The release writes it directly (owner role); the provisioning
    // job, which holds no table write, through the 0381 definer under its
    // lease.
    let result = match result {
        TenantResult::Migrated { applied_to_version } => {
            let recorded = match &lease {
                Some(l) => {
                    routing::record_schema_version_via_definer(
                        control,
                        &t.clerk_org_id,
                        &l.owner,
                        l.ttl,
                        applied_to_version,
                    )
                    .await
                }
                None => routing::set_schema_version(control, &t.clerk_org_id, applied_to_version).await,
            };
            match recorded
            {
                Ok(()) => TenantResult::Migrated { applied_to_version },
                Err(e) => TenantResult::Failed {
                    error: format!("tenant tree applied but schema_version was not recorded: {e}"),
                },
            }
        }
        other => other,
    };

    pool.close().await;
    mk(result)
}

/// Use Neon's documented direct endpoint for migration session locks only.
/// Runtime resolution remains unchanged. Parse first so credentials, TLS and
/// other connection options are never reconstructed or changed.
pub(crate) fn migration_connect_options(dsn: &str) -> Result<PgConnectOptions, sqlx::Error> {
    let options: PgConnectOptions = dsn.parse()?;
    let host = options.get_host();
    let Some((endpoint, domain)) = host.split_once('.') else {
        return Ok(options);
    };
    let valid_domain = domain.ends_with(".neon.tech")
        && domain.split('.').all(|label| {
            !label.is_empty()
                && label
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        });
    let Some(direct) = endpoint.strip_suffix("-pooler") else {
        return Ok(options);
    };
    if valid_domain
        && direct.starts_with("ep-")
        && direct.len() > 3
        && direct
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-')
    {
        let direct_host = format!("{direct}.{domain}");
        return Ok(options.host(&direct_host));
    }
    Ok(options)
}

async fn process_locked(
    pool: &PgPool,
    t: &TenantDatabase,
    mode: Mode,
    ddl_lock: DdlLockPolicy,
) -> TenantResult {
    // ONE PINNED CONNECTION for the whole lock lifetime.
    //
    // `pg_try_advisory_lock` is SESSION-scoped: the lock belongs to the
    // connection that took it. Running the lock, the work and the unlock as
    // three separate calls against a `PgPool` gave no guarantee they landed on
    // the same connection — the pool is sized `max_connections(2)`, so the
    // unlock could execute on the OTHER connection, where it is a no-op that
    // returns false. The lock then stayed held on the first connection until
    // that connection closed.
    //
    // That is not theoretical: on 2026-07-27 two production deploys aborted
    // with "advisory lock held by another runner" when no other runner was
    // running. The release_command machine is destroyed the moment the command
    // exits, so a lock leaked this way outlives the run that took it and
    // blocks the NEXT deploy until the server reaps the connection.
    let mut conn = match pool.acquire().await {
        Ok(c) => c,
        Err(e) => {
            return TenantResult::Failed {
                error: format!("acquire: {e}"),
            }
        }
    };

    // Try (non-blocking) advisory lock; if another runner holds it, skip.
    let got: bool = match sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
        .bind(ADVISORY_LOCK_KEY)
        .fetch_one(&mut *conn)
        .await
    {
        Ok(v) => v,
        Err(e) => {
            return TenantResult::Failed {
                error: format!("advisory lock: {e}"),
            }
        }
    };
    if !got {
        return TenantResult::Skipped {
            reason: "advisory lock held by another runner".into(),
        };
    }

    let result = match mode {
        Mode::Check => check_drift(&mut conn).await,
        Mode::Migrate => run_migrate(&mut conn, t, ddl_lock).await,
    };

    // The unlock's RESULT IS CHECKED. It was previously discarded with
    // `let _ =`, which made the failure above completely silent — no log, no
    // error, no metric, and the next run inheriting a held lock with nothing
    // in the record to explain it. `false` here means this session did not
    // hold the lock, which is an invariant violation rather than a hiccup:
    // something is wrong with the pinning above and the next deploy is about
    // to be blocked by it.
    match sqlx::query_scalar::<_, bool>("SELECT pg_advisory_unlock($1)")
        .bind(ADVISORY_LOCK_KEY)
        .fetch_one(&mut *conn)
        .await
    {
        Ok(true) => {}
        Ok(false) => tracing::error!(
            org = %t.clerk_org_id,
            "pg_advisory_unlock returned false — the lock was not held by this \
             session; it may now be leaked until this connection closes"
        ),
        Err(e) => tracing::error!(
            org = %t.clerk_org_id,
            error = %e,
            "failed to release the tenant migration advisory lock"
        ),
    }
    result
}

async fn run_migrate(
    conn: &mut PgConnection,
    t: &TenantDatabase,
    ddl_lock: DdlLockPolicy,
) -> TenantResult {
    // Apply the tenant tree (creates tenant_meta on first run; sqlx validates
    // the checksums of already-applied migrations and errors on drift). The
    // session is the pinned direct connection, so the lock policy's session
    // timeout cannot leak onto another borrower.
    if let Err(e) = apply_tree(&TENANT_MIGRATOR, &mut *conn, ddl_lock, &t.slug).await {
        return migrate_error_result(e);
    }
    // If the DB is already fingerprinted, it MUST match the org we think we are
    // migrating (defense against a swapped routing row / DSN). An empty
    // tenant_meta (freshly migrated, not yet fingerprinted by onboarding) is OK.
    match sqlx::query_scalar::<_, String>("SELECT org_id FROM tenant_meta LIMIT 1")
        .fetch_optional(&mut *conn)
        .await
    {
        Ok(Some(org)) if org != t.clerk_org_id => {
            return TenantResult::Failed {
                error: format!(
                    "fingerprint mismatch after migrate: expected {}, found {}",
                    t.clerk_org_id, org
                ),
            };
        }
        Ok(_) => {}
        Err(e) => {
            return TenantResult::Failed {
                error: format!("fingerprint read: {e}"),
            }
        }
    }
    TenantResult::Migrated {
        applied_to_version: head_version(),
    }
}

/// Classify a failed tree apply. sqlx refuses a database that records an
/// applied migration this build does not embed (`VersionMissing`, checked
/// before anything is applied). When that version is past this build's head
/// the database is AHEAD of this build (a newer build migrated it), which a
/// retry here can never fix: `AheadOfBuild`. A missing version at or below
/// the head is not that: this build's tree lost or renumbered a migration
/// the database already ran, so it is a plain failure saying so (parking
/// for a newer build would wait forever). Everything else is a plain failure.
fn migrate_error_result(e: sqlx::migrate::MigrateError) -> TenantResult {
    classify_migrate_error(e, head_version())
}

fn classify_migrate_error(e: sqlx::migrate::MigrateError, head: i64) -> TenantResult {
    match e {
        sqlx::migrate::MigrateError::VersionMissing(version) if version > head => {
            TenantResult::AheadOfBuild { version, head }
        }
        sqlx::migrate::MigrateError::VersionMissing(version) => TenantResult::Failed {
            error: format!(
                "migrate: the database records tenant migration {version}, which this build \
                 (head {head}) does not embed although it is not past the head; a migration \
                 was removed or renumbered in this build's tree"
            ),
        },
        other => TenantResult::Failed {
            error: format!("migrate: {other}"),
        },
    }
}

async fn check_drift(conn: &mut PgConnection) -> TenantResult {
    // Applied set from the tenant DB's own ledger. If the ledger table does not
    // exist yet (never migrated), everything is pending.
    let applied: HashMap<i64, Vec<u8>> = match sqlx::query_as::<_, (i64, Vec<u8>)>(
        "SELECT version, checksum FROM _sqlx_migrations ORDER BY version",
    )
    .fetch_all(&mut *conn)
    .await
    {
        Ok(rows) => rows.into_iter().collect(),
        Err(_) => HashMap::new(),
    };

    let mut issues: Vec<String> = Vec::new();
    let mut embedded_versions: HashSet<i64> = HashSet::new();
    for m in TENANT_MIGRATOR.iter() {
        embedded_versions.insert(m.version);
        match applied.get(&m.version) {
            None => issues.push(format!("pending {}", m.version)),
            Some(cs) if cs.as_slice() != m.checksum.as_ref() => {
                issues.push(format!("checksum-drift {}", m.version))
            }
            _ => {}
        }
    }
    for v in applied.keys() {
        if !embedded_versions.contains(v) {
            issues.push(format!("unknown-applied {v}"));
        }
    }

    if issues.is_empty() {
        TenantResult::UpToDate {
            version: head_version(),
        }
    } else {
        TenantResult::Drift {
            detail: issues.join(", "),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_uses_direct_neon_without_changing_auth_tls_or_application_name() {
        use sqlx::ConnectOptions;
        let dsn = "postgresql://fixture%40owner:p%40ss%3Aword@ep-coral-123-pooler.us-east-2.aws.neon.tech:5544/qa_fixture?sslmode=verify-full&application_name=tenant-migrator";
        let original: PgConnectOptions = dsn.parse().unwrap();
        let direct = migration_connect_options(dsn).unwrap();
        assert_eq!(direct.get_host(), "ep-coral-123.us-east-2.aws.neon.tech");
        assert_eq!(direct.get_username(), original.get_username());
        assert_eq!(
            direct.to_url_lossy().password(),
            original.to_url_lossy().password()
        );
        assert_eq!(direct.get_database(), original.get_database());
        assert_eq!(direct.get_port(), original.get_port());
        assert!(matches!(
            direct.get_ssl_mode(),
            sqlx::postgres::PgSslMode::VerifyFull
        ));
        assert!(matches!(
            original.get_ssl_mode(),
            sqlx::postgres::PgSslMode::VerifyFull
        ));
        assert_eq!(
            direct.get_application_name(),
            original.get_application_name()
        );
        assert!(original.get_options().is_none(), "pooler DSNs must omit startup GUCs");
        assert!(direct.get_options().is_none(), "migration URL must not add startup GUCs");
        // The runtime resolver still supplies the original pooled value.
        let resolver =
            super::super::secrets::StaticMapResolver::from_pairs([("TENANT_DB_QA", dsn)]);
        assert_eq!(resolver.resolve("TENANT_DB_QA").unwrap(), dsn);
    }

    #[test]
    fn migration_does_not_rewrite_direct_or_unrecognized_hosts() {
        for host in [
            "ep-coral.us-east-2.aws.neon.tech",
            "localhost",
            "custom-pooler.example.com",
            "ep-coral-pooler.neon.tech.evil.example",
            "ep-coral-pooler.neon.tech",
            "notendpoint-pooler.us.aws.neon.tech",
            "ep--pooler.us.aws.neon.tech",
        ] {
            let dsn = format!("postgres://fixture:synthetic@{host}/qa");
            assert_eq!(migration_connect_options(&dsn).unwrap().get_host(), host);
        }
    }

    #[test]
    fn migration_rejects_invalid_dsn() {
        assert!(migration_connect_options("not a database URL").is_err());
    }

    fn outcome(slug: &str, result: TenantResult) -> TenantOutcome {
        TenantOutcome {
            clerk_org_id: format!("org_{slug}"),
            slug: slug.to_string(),
            result,
        }
    }

    /// The release command gates on these three predicates, so they must stay
    /// independent: a skip is neither a failure nor a success.
    #[test]
    fn counts_and_predicates_separate_skip_from_failure() {
        let report = MigrateReport {
            outcomes: vec![
                outcome(
                    "a",
                    TenantResult::Migrated {
                        applied_to_version: 7,
                    },
                ),
                outcome("b", TenantResult::UpToDate { version: 7 }),
                outcome(
                    "c",
                    TenantResult::Skipped {
                        reason: "lock".into(),
                    },
                ),
                outcome(
                    "d",
                    TenantResult::Failed {
                        error: "connect".into(),
                    },
                ),
                outcome(
                    "e",
                    TenantResult::Drift {
                        detail: "pending 7".into(),
                    },
                ),
            ],
        };
        let c = report.counts();
        assert_eq!(c.migrated, 1);
        assert_eq!(c.up_to_date, 1);
        assert_eq!(c.skipped, 1);
        assert_eq!(c.failed, 1);
        assert_eq!(c.drifted, 1);
        assert_eq!(
            c.total(),
            5,
            "every outcome must land in exactly one bucket"
        );
        assert!(report.any_failed());
        assert!(report.any_skipped());
    }

    #[test]
    fn skip_alone_is_not_a_failure_but_is_not_silent() {
        let report = MigrateReport {
            outcomes: vec![
                outcome("a", TenantResult::UpToDate { version: 7 }),
                outcome(
                    "b",
                    TenantResult::Skipped {
                        reason: "lock".into(),
                    },
                ),
            ],
        };
        assert!(!report.any_failed(), "lock contention is not an error");
        assert!(
            report.any_skipped(),
            "…but the caller must still be able to tell this run apart from a clean one"
        );
        assert_eq!(report.counts().skipped, 1);
    }

    #[test]
    fn a_database_ahead_of_this_build_is_its_own_outcome_and_still_fails_the_release() {
        let head = head_version();
        match migrate_error_result(sqlx::migrate::MigrateError::VersionMissing(head + 1)) {
            TenantResult::AheadOfBuild { version, head: h } => assert_eq!((version, h), (head + 1, head)),
            other => panic!("VersionMissing past the head must map to AheadOfBuild, got {other:?}"),
        }
        assert!(matches!(
            migrate_error_result(sqlx::migrate::MigrateError::Dirty(3)),
            TenantResult::Failed { .. }
        ));
        let report = MigrateReport {
            outcomes: vec![outcome("ahead", TenantResult::AheadOfBuild { version: head + 1, head })],
        };
        assert!(report.any_failed(), "the release still fails for a tenant it cannot bring to head");
        assert_eq!(report.counts().failed, 1);
    }

    #[test]
    fn a_missing_version_at_or_below_the_head_is_a_removed_migration_not_a_newer_build() {
        let head = 20_250_101_000_000;
        for missing in [head - 1, head] {
            match classify_migrate_error(sqlx::migrate::MigrateError::VersionMissing(missing), head) {
                TenantResult::Failed { error } => {
                    assert!(error.contains(&missing.to_string()), "{error}");
                    assert!(error.contains("removed or renumbered"), "{error}");
                }
                other => panic!("VersionMissing({missing}) at or below head {head} must be Failed, got {other:?}"),
            }
        }
        match classify_migrate_error(sqlx::migrate::MigrateError::VersionMissing(head + 1), head) {
            TenantResult::AheadOfBuild { version, head: h } => assert_eq!((version, h), (head + 1, head)),
            other => panic!("VersionMissing past the head must map to AheadOfBuild, got {other:?}"),
        }
    }

    #[test]
    fn empty_run_is_clean() {
        let report = MigrateReport { outcomes: vec![] };
        assert!(!report.any_failed());
        assert!(!report.any_skipped());
        assert_eq!(report.counts().total(), 0);
    }
}
