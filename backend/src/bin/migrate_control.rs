//! `migrate_control` — applies the CONTROL migration tree (identity,
//! accounts, routing) to the control-plane database.
//!
//! Run out-of-band (deploy release step or manually), NEVER at web-server
//! boot. Modelled on its sibling `migrate_tenants`, which has always applied
//! the per-org TENANT tree this way; this binary closes the same gap for the
//! control tree.
//!
//! Why it exists (OPS-4): migration `0083_consumer_rls` REVOKEs ALL on
//! `_sqlx_migrations` from the least-privilege `coralswarm_app` role, so
//! applying migrations requires the elevated owner role. Doing that at boot
//! meant the *serving* process had to hold an elevated DSN in its environment
//! for its whole lifetime — a post-boot compromise of the server could read it
//! and connect as the owner, bypassing RLS and regaining the schema-drop /
//! `tenant_databases`-rewrite powers OPS-4 deliberately removes. Applying the
//! tree from a separate, short-lived process means the serving container never
//! needs the elevated credential at all.
//!
//! It also fixes an ordering hazard: N booting machines used to race the same
//! migration behind sqlx's advisory lock. As a Fly `release_command` this runs
//! to completion *before* any traffic is routed to the new image, and a
//! non-zero exit aborts the deploy.
//!
//! Usage:
//!   migrate_control [--check]
//!
//! Env:
//!   MIGRATOR_DATABASE_URL  elevated (owner-role) control-plane DSN. Preferred.
//!   DATABASE_URL           fallback when MIGRATOR_DATABASE_URL is unset/blank
//!                          — the pre-OPS-4 config, where DATABASE_URL still
//!                          IS the owner role.
//!   At least one of the two must be set.
//!
//! Exit codes: 0 success / up-to-date, 1 failure (or drift under `--check`),
//! 2 usage or configuration error.
//!
//! Examples:
//!   # dev / CI, against a fresh database
//!   DATABASE_URL=postgres://localhost:5432/coralswarm_dev \
//!     cargo run -p coralswarm-backend --bin migrate_control
//!
//!   # prod (Fly runs this for you as the deploy release_command)
//!   MIGRATOR_DATABASE_URL=<owner DSN> /app/migrate_control

use std::collections::{HashMap, HashSet};
use std::process::ExitCode;
use std::time::Duration;

use coralswarm_backend::tenancy::migrate::{self, DdlLockPolicy};
use coralswarm_backend::{control_head_version, migrator_database_url, CONTROL_MIGRATOR};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

/// Connect timeout for the one-shot migration connection. Matches
/// `migrate_tenants`' 15s: long enough for a cold Fly Postgres, short enough
/// that a misconfigured DSN aborts the deploy quickly instead of hanging it.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// Apply pending control migrations.
    Migrate,
    /// Report drift (pending / checksum mismatch / unknown-applied) without
    /// mutating. Exits non-zero if anything drifted.
    Check,
}

const USAGE: &str = "usage: migrate_control [--check]";

// Catalog metadata only: no protected table rows, credentials or DSNs. A
// failed migration rolls back billing_private, so absent objects stay NULL
// rather than falsely reporting that their permissions were safe in-flight.
const AUTHORITY_DIAGNOSTIC: &str = r#"
WITH objects AS (
 SELECT (SELECT oid FROM pg_namespace WHERE nspname='public') AS public_schema,
        (SELECT oid FROM pg_namespace WHERE nspname='billing_private') AS billing_schema,
        to_regclass('billing_private.account_entry') AS billing_entry,
        (SELECT oid FROM pg_roles WHERE rolname='coralswarm_billing_entry_owner') AS billing_owner,
        (SELECT datdba FROM pg_database WHERE datname=current_database()) AS database_owner
), apps AS (
 SELECT expected.name,r.* FROM (VALUES ('coralswarm_app'),('coralswarm_tenant_app')) expected(name)
 LEFT JOIN pg_roles r ON r.rolname=expected.name
)
SELECT jsonb_build_object('roles',jsonb_agg(jsonb_build_object(
 'role',a.name,'exists',a.oid IS NOT NULL,
 'public_create',has_schema_privilege(a.oid,o.public_schema,'CREATE'),
 'public_schema_owner',(SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE oid=o.public_schema),
 'database_owner',pg_get_userbyid(o.database_owner),'is_database_owner',a.oid=o.database_owner,
 'public_create_acl',(SELECT COALESCE(jsonb_agg(jsonb_build_object(
   'grantee',CASE WHEN acl.grantee=0 THEN 'PUBLIC' ELSE pg_get_userbyid(acl.grantee) END,
   'grantor',pg_get_userbyid(acl.grantor),'grantable',acl.is_grantable)),'[]'::jsonb)
   FROM pg_namespace n CROSS JOIN LATERAL aclexplode(COALESCE(n.nspacl,acldefault('n',n.nspowner))) acl
   WHERE n.oid=o.public_schema AND acl.privilege_type='CREATE'),
 'member_of',(SELECT COALESCE(jsonb_agg(r.rolname ORDER BY r.rolname),'[]'::jsonb)
   FROM pg_roles r WHERE r.oid<>a.oid AND pg_has_role(a.oid,r.oid,'MEMBER')),
 'role_flags',jsonb_build_object('superuser',a.rolsuper,'createdb',a.rolcreatedb,
   'createrole',a.rolcreaterole,'replication',a.rolreplication,'bypassrls',a.rolbypassrls),
 'billing_schema_exists',o.billing_schema IS NOT NULL,'billing_entry_exists',o.billing_entry IS NOT NULL,
 'billing_owner_exists',o.billing_owner IS NOT NULL,
 'billing_schema_usage',has_schema_privilege(a.oid,o.billing_schema,'USAGE'),
 'billing_schema_create',has_schema_privilege(a.oid,o.billing_schema,'CREATE'),
 'billing_entry_any_privilege',has_table_privilege(a.oid,o.billing_entry,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'),
 'billing_owner_member',pg_has_role(a.oid,o.billing_owner,'MEMBER')
) ORDER BY a.name)) FROM apps a CROSS JOIN objects o
"#;

async fn report_migration_321_authority(pool: &PgPool) {
    // Bound acquisition, sampling and cleanup independently. Even a diagnostic
    // failure must preserve the original migration failure and never retry it.
    let Ok(Ok(mut tx)) = tokio::time::timeout(Duration::from_secs(2), pool.begin()).await else {
        eprintln!("migration-321 authority diagnostic: unavailable (connection)");
        return;
    };
    let sample = tokio::time::timeout(Duration::from_secs(3), async {
        sqlx::query("SET TRANSACTION READ ONLY")
            .execute(&mut *tx)
            .await?;
        sqlx::query("SET LOCAL statement_timeout='2s'")
            .execute(&mut *tx)
            .await?;
        sqlx::query_scalar::<_, serde_json::Value>(AUTHORITY_DIAGNOSTIC)
            .fetch_one(&mut *tx)
            .await
    })
    .await;
    let rollback = tokio::time::timeout(Duration::from_secs(1), tx.rollback()).await;
    match (sample, rollback) {
        (Ok(Ok(value)), Ok(Ok(()))) => eprintln!("migration-321 authority diagnostic: {value}"),
        _ => eprintln!(
            "migration-321 authority diagnostic: unavailable (catalog sample or rollback)"
        ),
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let mode = match parse_args() {
        Ok(m) => m,
        Err(msg) => {
            eprintln!("{msg}");
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    };

    // MIGRATOR_DATABASE_URL wins; DATABASE_URL is the pre-OPS-4 fallback.
    // Resolution lives in the library (`resolve_migrator_url`) so its
    // precedence/blank-handling rules carry unit coverage.
    let url = match migrator_database_url() {
        Some(u) => u,
        None => {
            eprintln!(
                "MIGRATOR_DATABASE_URL (preferred) or DATABASE_URL must be set to an elevated \
                 control-plane DSN — the role must own `_sqlx_migrations` (post-OPS-4 the \
                 least-privilege `coralswarm_app` role deliberately cannot write it)"
            );
            return ExitCode::from(2);
        }
    };

    // A single connection is enough — sqlx's migrator serializes on its own
    // advisory lock anyway, and max_connections=2 just gives that lock a
    // little headroom (same shape as the old boot-time migrator pool).
    let pool = match PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(CONNECT_TIMEOUT)
        .connect(&url)
        .await
    {
        Ok(p) => p,
        Err(e) => {
            eprintln!(
                "failed to connect control-plane database — check \
                 MIGRATOR_DATABASE_URL/DATABASE_URL: {e}"
            );
            return ExitCode::FAILURE;
        }
    };

    let head = control_head_version();
    let code = match mode {
        Mode::Check => check(&pool, head).await,
        Mode::Migrate => migrate(&pool, head).await,
    };

    pool.close().await;
    code
}

fn parse_args() -> Result<Mode, String> {
    let mut mode = Mode::Migrate;
    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "--check" => mode = Mode::Check,
            "-h" | "--help" => {
                println!("{USAGE}");
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    Ok(mode)
}

/// Applied `version -> checksum` from the database's own ledger. A missing
/// ledger table (never-migrated database) is not an error here — it simply
/// means "nothing applied yet"; any real permission/connectivity problem
/// surfaces from the migrate/check step immediately after.
async fn applied_versions(pool: &PgPool) -> HashMap<i64, Vec<u8>> {
    match sqlx::query_as::<_, (i64, Vec<u8>)>(
        "SELECT version, checksum FROM _sqlx_migrations ORDER BY version",
    )
    .fetch_all(pool)
    .await
    {
        Ok(rows) => rows.into_iter().collect(),
        Err(_) => HashMap::new(),
    }
}

// Older PostgreSQL databases may retain PUBLIC's legacy schema CREATE grant.
// Remove only that ambient grant before applying the immutable billing guard;
// owner/named-role privileges and PUBLIC USAGE remain unchanged.
async fn remove_legacy_public_create(pool: &PgPool) -> Result<bool, sqlx::Error> {
    let mut tx = tokio::time::timeout(Duration::from_secs(2), pool.begin())
        .await
        .map_err(|_| sqlx::Error::PoolTimedOut)??;
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        sqlx::query("SET LOCAL statement_timeout='2s'").execute(&mut *tx).await?;
        sqlx::query("SET LOCAL lock_timeout='2s'").execute(&mut *tx).await?;
        let legacy_sql = "SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_namespace n \
            CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(n.nspacl,pg_catalog.acldefault('n',n.nspowner))) acl \
            WHERE n.nspname='public' AND acl.grantee=0 AND acl.privilege_type='CREATE')";
        let legacy: bool = sqlx::query_scalar(legacy_sql).fetch_one(&mut *tx).await?;
        if legacy {
            sqlx::query("REVOKE CREATE ON SCHEMA public FROM PUBLIC").execute(&mut *tx).await?;
            // REVOKE can warn and succeed without changing an ACL when the
            // migrator lacks grant authority. Verify the actual catalog effect.
            let remains: bool = sqlx::query_scalar(legacy_sql).fetch_one(&mut *tx).await?;
            if remains {
                return Err(sqlx::Error::Protocol("PUBLIC CREATE remains after schema hardening".into()));
            }
        }
        Ok::<_, sqlx::Error>(legacy)
    }).await.unwrap_or(Err(sqlx::Error::PoolTimedOut));
    match result {
        Ok(changed) => {
            tokio::time::timeout(Duration::from_secs(2), tx.commit())
                .await
                .map_err(|_| sqlx::Error::PoolTimedOut)??;
            Ok(changed)
        }
        Err(error) => {
            let _ = tokio::time::timeout(Duration::from_secs(1), tx.rollback()).await;
            Err(error)
        }
    }
}

async fn migrate(pool: &PgPool, head: i64) -> ExitCode {
    match remove_legacy_public_create(pool).await {
        Ok(true) => println!("control preflight: revoked legacy PUBLIC CREATE on schema public"),
        Ok(false) => {}
        Err(error) => {
            eprintln!("control preflight failed while removing legacy PUBLIC CREATE: {error}");
            return ExitCode::FAILURE;
        }
    }
    let before = applied_versions(pool).await;

    // sqlx validates the checksums of already-applied migrations as part of
    // `run()` and errors on drift, so a tampered/rewritten migration fails the
    // deploy here rather than silently diverging (the same immutability
    // contract `scripts/check_migrations.sh` enforces at review time).
    //
    // One pinned session under the tenant migrator's DDL lock policy: a
    // migration's lock wait on a hot control table is bounded and retried
    // instead of queueing every writer behind it. This pool is process-
    // private and connects to Fly Postgres directly, so the session-level
    // lock_timeout cannot reach another borrower.
    let applied: Result<(), sqlx::migrate::MigrateError> = async {
        let mut conn = pool.acquire().await?;
        migrate::apply_tree(&CONTROL_MIGRATOR, &mut conn, DdlLockPolicy::default(), "control").await
    }
    .await;
    if let Err(e) = applied {
        eprintln!("control-tree migration failed: {e}");
        if matches!(&e, sqlx::migrate::MigrateError::ExecuteMigration(_, 321)) {
            report_migration_321_authority(pool).await;
        }
        return ExitCode::FAILURE;
    }

    let after = applied_versions(pool).await;
    let mut newly: Vec<i64> = after
        .keys()
        .filter(|v| !before.contains_key(v))
        .copied()
        .collect();
    newly.sort_unstable();

    if newly.is_empty() {
        println!("control tree: up-to-date (v{head}, {} applied)", after.len());
    } else {
        let list = newly
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        println!(
            "control tree: applied {} migration(s) [{list}] → v{head}",
            newly.len()
        );
    }
    ExitCode::SUCCESS
}

async fn check(pool: &PgPool, head: i64) -> ExitCode {
    let applied = applied_versions(pool).await;

    let mut issues: Vec<String> = Vec::new();
    let mut embedded: HashSet<i64> = HashSet::new();
    for m in CONTROL_MIGRATOR.iter() {
        embedded.insert(m.version);
        match applied.get(&m.version) {
            None => issues.push(format!("pending {}", m.version)),
            Some(cs) if cs.as_slice() != m.checksum.as_ref() => {
                issues.push(format!("checksum-drift {}", m.version))
            }
            _ => {}
        }
    }
    // An applied version this build has never heard of means the database was
    // migrated by a NEWER binary — deploying this image would serve queries
    // against a schema ahead of it.
    let mut unknown: Vec<i64> = applied
        .keys()
        .filter(|v| !embedded.contains(v))
        .copied()
        .collect();
    unknown.sort_unstable();
    for v in unknown {
        issues.push(format!("unknown-applied {v}"));
    }

    if issues.is_empty() {
        println!("control tree: up-to-date (v{head})");
        ExitCode::SUCCESS
    } else {
        eprintln!("control tree: DRIFT — {}", issues.join(", "));
        ExitCode::FAILURE
    }
}
