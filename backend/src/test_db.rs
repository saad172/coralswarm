//! Test-only: migrated scratch databases cloned from a template.
//!
//! Running all 200+ migrations for every test dominated the pg suites
//! (13-22 min for the Library suites). [`migrated_database`] migrates one
//! template per plane and migration set, then gives each caller a
//! `CREATE DATABASE ... TEMPLATE` copy, which is a file copy.
//!
//! - The template name carries a hash of every embedded migration's version,
//!   description and checksum, so an edited or added migration builds a new
//!   template and a stale one is never reused. Older templates of the same
//!   plane are dropped when a new one is built.
//! - The template is built under a scratch name and renamed only once fully
//!   migrated, so a crash mid-build never leaves a half-migrated template.
//! - The template is marked `ALLOW_CONNECTIONS false`: Postgres refuses to
//!   copy a database that has other sessions, and nothing should connect.
//! - Per-database settings (`ALTER DATABASE ... SET`, e.g. tenant 0092's
//!   `search_path`) live in the cluster catalog, keyed by database OID, so a
//!   template copy does not inherit them. They are copied explicitly.
//! - Roles are cluster-wide; the migrations create them while building the
//!   template, exactly as a fresh migration run would.
//!
//! Every step runs under one cluster-wide advisory lock, so concurrent test
//! binaries build the template once and never copy it mid-build.

use sha2::{Digest, Sha256};
use sqlx::{
    migrate::Migrator,
    postgres::{PgConnectOptions, PgPoolOptions},
    Connection, Executor, PgConnection,
};
use uuid::Uuid;

static TENANT: Migrator = sqlx::migrate!("./migrations_tenant");
static CONTROL: Migrator = sqlx::migrate!("./migrations_control");

/// Advisory lock key for building and copying templates.
const TEMPLATE_LOCK: i64 = 0x7E57_DB7E;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Plane {
    Tenant,
    Control,
}

impl Plane {
    fn migrator(self) -> &'static Migrator {
        match self {
            Plane::Tenant => &TENANT,
            Plane::Control => &CONTROL,
        }
    }

    fn tag(self) -> &'static str {
        match self {
            Plane::Tenant => "tenant",
            Plane::Control => "control",
        }
    }
}

/// `cs_tpl_<plane>_<16 hex>`, unique to this exact migration set.
pub fn template_name(plane: Plane) -> String {
    let mut hasher = Sha256::new();
    for migration in plane.migrator().iter() {
        hasher.update(migration.version.to_le_bytes());
        hasher.update(migration.description.as_bytes());
        hasher.update(&migration.checksum);
        hasher.update([0]);
    }
    let digest = hasher.finalize();
    let hex: String = digest[..8].iter().map(|b| format!("{b:02x}")).collect();
    format!("cs_tpl_{}_{hex}", plane.tag())
}

/// Creates a fully migrated database for `plane` named
/// [`unique_name`]`(prefix)` and returns its guard, which drops it.
pub async fn migrated_database(base: &PgConnectOptions, plane: Plane, prefix: &str) -> TestDatabase {
    let name = TestDatabase::new(base, prefix);
    let mut admin = PgConnection::connect_with(base).await.expect("connect to the test Postgres");
    sqlx::query("SELECT pg_advisory_lock($1)").bind(TEMPLATE_LOCK).execute(&mut admin).await.unwrap();
    let template = template_name(plane);
    ensure_template(&mut admin, base, plane, &template).await;
    admin.execute(format!("CREATE DATABASE {name} TEMPLATE {template}").as_str()).await.unwrap();
    copy_database_settings(&mut admin, &template, &name).await;
    sqlx::query("SELECT pg_advisory_unlock($1)").bind(TEMPLATE_LOCK).execute(&mut admin).await.unwrap();
    admin.close().await.ok();
    name
}

async fn ensure_template(admin: &mut PgConnection, base: &PgConnectOptions, plane: Plane, template: &str) {
    let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)")
        .bind(template)
        .fetch_one(&mut *admin)
        .await
        .unwrap();
    if exists {
        return;
    }
    let building = format!("{template}_build");
    admin.execute(format!("DROP DATABASE IF EXISTS {building} WITH (FORCE)").as_str()).await.unwrap();
    admin.execute(format!("CREATE DATABASE {building}").as_str()).await.unwrap();
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(base.clone().database(&building))
        .await
        .unwrap();
    plane.migrator().run(&pool).await.expect("migrate the template database");
    pool.close().await;
    admin.execute(format!("ALTER DATABASE {building} RENAME TO {template}").as_str()).await.unwrap();
    admin
        .execute(format!("ALTER DATABASE {template} WITH IS_TEMPLATE true ALLOW_CONNECTIONS false").as_str())
        .await
        .unwrap();
    let stale: Vec<String> = sqlx::query_scalar(
        "SELECT datname FROM pg_database WHERE datname LIKE $1 AND datname<>$2 AND datistemplate",
    )
    .bind(format!("cs\\_tpl\\_{}\\_%", plane.tag()))
    .bind(template)
    .fetch_all(&mut *admin)
    .await
    .unwrap();
    for old in stale {
        admin.execute(format!("ALTER DATABASE {old} WITH IS_TEMPLATE false").as_str()).await.unwrap();
        admin.execute(format!("DROP DATABASE IF EXISTS {old} WITH (FORCE)").as_str()).await.unwrap();
    }
}

/// Re-applies the template's `ALTER DATABASE / ALTER ROLE ... IN DATABASE`
/// settings to the copy; `CREATE DATABASE ... TEMPLATE` does not carry them.
async fn copy_database_settings(admin: &mut PgConnection, template: &str, copy: &str) {
    let rows: Vec<(Option<String>, Vec<String>)> = sqlx::query_as(
        "SELECT r.rolname::text, s.setconfig FROM pg_db_role_setting s \
         JOIN pg_database d ON d.oid=s.setdatabase LEFT JOIN pg_roles r ON r.oid=s.setrole \
         WHERE d.datname=$1",
    )
    .bind(template)
    .fetch_all(&mut *admin)
    .await
    .unwrap();
    for (role, settings) in &rows {
        for setting in settings {
            let (key, value) = setting.split_once('=').expect("pg_db_role_setting entry is key=value");
            let target = match role {
                Some(role) => format!("ROLE \"{role}\" IN DATABASE {copy}"),
                None => format!("DATABASE {copy}"),
            };
            // List settings (search_path) are stored already flattened into
            // re-parseable SQL; quoting them would make one list element.
            // Scalar settings are stored raw and need a literal.
            let value = if value.contains(',') || value.contains('"') {
                value.to_owned()
            } else {
                format!("'{}'", value.replace('\'', "''"))
            };
            admin.execute(format!("ALTER {target} SET {key} = {value}").as_str()).await.unwrap();
        }
    }
    // Self-check: the copy must carry exactly the template's settings.
    let copied: Vec<(Option<String>, Vec<String>)> = sqlx::query_as(
        "SELECT r.rolname::text, s.setconfig FROM pg_db_role_setting s \
         JOIN pg_database d ON d.oid=s.setdatabase LEFT JOIN pg_roles r ON r.oid=s.setrole \
         WHERE d.datname=$1",
    )
    .bind(copy)
    .fetch_all(&mut *admin)
    .await
    .unwrap();
    let mut expected = rows;
    let mut actual = copied;
    expected.sort();
    actual.sort();
    assert_eq!(actual, expected, "template copy {copy} lost per-database settings of {template}");
}

/// Every database and role a test creates through this module starts with
/// this marker, so [`sweep`] can tell test leftovers from anything else.
pub const MARKER: &str = "cstest_";

/// Set to `1` to keep test databases and roles for inspection instead of
/// dropping them (the same switch `tests/support/retained_database.rs` reads).
const RETAIN_ENV: &str = "ORGT_TEST_RETAIN_DATABASES";

/// `cstest_<prefix>_<unix seconds, 8 hex>_<12 hex nonce>`, with the prefix cut
/// to fit 63 bytes. The creation time is in the name because Postgres records
/// none, and [`sweep`] needs it.
pub fn unique_name(prefix: &str) -> String {
    assert!(
        !prefix.is_empty() && prefix.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_'),
        "test database prefix {prefix:?} must be [a-z0-9_]+"
    );
    let created = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after 1970")
        .as_secs();
    let nonce = Uuid::new_v4().simple().to_string();
    // Postgres truncates identifiers past 63 bytes, so a later connect would
    // miss; the prefix is only a label, so it is the part that gets cut.
    let prefix = &prefix[..prefix.len().min(63 - MARKER.len() - "_01234567_0123456789ab".len())];
    format!("{MARKER}{prefix}_{created:08x}_{}", &nonce[..12])
}

/// The creation time [`unique_name`] encoded, or `None` for any other name.
fn created_at(name: &str) -> Option<u64> {
    if name.len() > 63 || !name.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_') {
        return None;
    }
    let rest = name.strip_prefix(MARKER)?;
    let (rest, nonce) = rest.rsplit_once('_')?;
    let (_, created) = rest.rsplit_once('_')?;
    let hex = |s: &str, len: usize| s.len() == len && s.bytes().all(|b| b.is_ascii_hexdigit());
    (hex(nonce, 12) && hex(created, 8)).then(|| u64::from_str_radix(created, 16).ok()).flatten()
}

fn assert_identifier(name: &str) {
    assert!(
        !name.is_empty() && name.len() <= 63 && name.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_'),
        "test identifier {name:?} must be [a-z0-9_]{{1,63}}"
    );
}

/// A per-test database (and the per-test roles registered on it) that is
/// dropped when the guard is dropped: at the end of the test, and also when
/// the test panics, fails or times out inside the harness.
///
/// The guard only reserves the name; the caller still runs its own
/// `CREATE DATABASE {db} ...`, so `OWNER`/`TEMPLATE` clauses stay at the call
/// site. It derefs to the name, so `&db`, `format!("{db}")` and
/// `options.database(&db)` keep working. Keep it alive for the whole test, and
/// declare it before the pools that connect to it so they close first;
/// `DROP DATABASE ... WITH (FORCE)` ends any session still open.
///
/// `Drop` is synchronous, so it connects on a fresh thread with its own
/// current-thread runtime (blocking inside the test's runtime would deadlock
/// a current-thread `#[tokio::test]`). It never panics: a failed drop is
/// printed to stderr and left for [`sweep`].
///
/// A killed run (SIGKILL, Ctrl-C, a CI timeout) runs no destructors. Those
/// leftovers carry [`MARKER`] and a creation time; sweep them with
///
/// ```text
/// ORGT_TEST_BASE_URL=postgres://...localhost.../postgres \
///   cargo test -p coralswarm-backend --test pg_test_db_sweep -- --ignored --nocapture
/// ```
///
/// which drops marked databases older than `CSTEST_SWEEP_MIN_AGE_MINUTES`
/// (default 60) that have no open sessions, then marked roles of the same age.
/// `ORGT_TEST_RETAIN_DATABASES=1` keeps everything for inspection instead.
pub struct TestDatabase {
    admin: PgConnectOptions,
    name: String,
    roles: std::sync::Mutex<Vec<String>>,
}

impl TestDatabase {
    /// Reserves [`unique_name`]`(prefix)` on the server `admin` points at.
    pub fn new(admin: &PgConnectOptions, prefix: &str) -> Self {
        Self::adopt(admin, unique_name(prefix))
    }

    /// Takes ownership of a name the test must choose itself (a product
    /// naming rule, a fixed scratch name). It is dropped like any other, but
    /// carries no marker, so [`sweep`] cannot find it after a killed run.
    pub fn adopt(admin: &PgConnectOptions, name: impl Into<String>) -> Self {
        let name = name.into();
        assert_identifier(&name);
        assert!(
            matches!(admin.get_host(), "localhost" | "127.0.0.1" | "::1"),
            "test databases are created on a local Postgres only (#742)"
        );
        // Cleanup must not reconnect to another scratch database that may drop first.
        Self { admin: admin.clone().database("postgres"), name, roles: Default::default() }
    }

    /// Owns a fixed test database name using an explicit maintenance URL.
    /// Fixed names cannot be age-swept, but still clean up on test failure.
    pub fn adopt_from_url(admin_url: &str, name: &str) -> Self {
        Self::adopt(&admin_url.parse().expect("test Postgres URL"), name)
    }

    /// Owns a fixed-name fixture when its explicit test-server variable is set.
    /// Returns `None` for the existing DSN-less test skip path.
    pub fn adopt_from_env(variable: &str, name: &str) -> Option<Self> {
        std::env::var(variable).ok().map(|url| Self::adopt_from_url(&url, name))
    }

    /// [`TestDatabase::new`] from a connection URL.
    pub fn from_url(admin_url: &str, prefix: &str) -> Self {
        Self::new(&admin_url.parse().expect("test Postgres URL"), prefix)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Reserves [`unique_name`]`(prefix)` for a per-test role, dropped after
    /// this database (so it no longer owns anything in it).
    pub fn role(&self, prefix: &str) -> String {
        let role = unique_name(prefix);
        self.adopt_role(&role);
        role
    }

    /// Registers a role the test named itself, dropped after this database.
    pub fn adopt_role(&self, role: &str) {
        assert_identifier(role);
        self.roles.lock().unwrap_or_else(|e| e.into_inner()).push(role.to_owned());
    }
}

impl std::ops::Deref for TestDatabase {
    type Target = str;
    fn deref(&self) -> &str {
        &self.name
    }
}

impl AsRef<str> for TestDatabase {
    fn as_ref(&self) -> &str {
        &self.name
    }
}

impl std::fmt::Display for TestDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.name)
    }
}

impl std::fmt::Debug for TestDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("TestDatabase").field(&self.name).finish()
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        let roles = std::mem::take(&mut *self.roles.lock().unwrap_or_else(|e| e.into_inner()));
        drop_blocking(&self.admin, Some(&self.name), &roles);
    }
}

/// A per-test role with no database of its own, dropped with the guard.
/// Register a role on its [`TestDatabase`] instead when it owns objects there.
pub struct TestRole {
    admin: PgConnectOptions,
    name: String,
}

impl TestRole {
    /// Reserves [`unique_name`]`(prefix)`; the caller runs its own `CREATE ROLE`.
    pub fn new(admin: &PgConnectOptions, prefix: &str) -> Self {
        let name = unique_name(prefix);
        assert!(
            matches!(admin.get_host(), "localhost" | "127.0.0.1" | "::1"),
            "test roles are created on a local Postgres only (#742)"
        );
        Self { admin: admin.clone().database("postgres"), name }
    }

    /// [`TestRole::new`] from a connection URL.
    pub fn from_url(admin_url: &str, prefix: &str) -> Self {
        Self::new(&admin_url.parse().expect("test Postgres URL"), prefix)
    }
}

impl std::ops::Deref for TestRole {
    type Target = str;
    fn deref(&self) -> &str {
        &self.name
    }
}

impl std::fmt::Display for TestRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.name)
    }
}

impl Drop for TestRole {
    fn drop(&mut self) {
        drop_blocking(&self.admin, None, std::slice::from_ref(&self.name));
    }
}

fn drop_blocking(admin: &PgConnectOptions, database: Option<&str>, roles: &[String]) {
    let what = database.map_or_else(|| format!("roles {roles:?}"), |db| format!("database {db} and roles {roles:?}"));
    if std::env::var(RETAIN_ENV).as_deref() == Ok("1") {
        eprintln!("test_db: {RETAIN_ENV}=1, retaining {what}");
        return;
    }
    let admin = admin.clone();
    let database = database.map(str::to_owned);
    let roles = roles.to_vec();
    let worker = std::thread::spawn(move || -> Result<(), String> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("runtime: {e}"))?;
        runtime.block_on(async {
            tokio::time::timeout(std::time::Duration::from_secs(60), drop_now(&admin, database.as_deref(), &roles))
                .await
                .map_err(|_| "timed out after 60s".to_owned())?
        })
    });
    match worker.join() {
        Ok(Ok(())) => {}
        Ok(Err(e)) => eprintln!("test_db: failed to drop {what}: {e}; `pg_test_db_sweep` removes marked leftovers"),
        Err(_) => eprintln!("test_db: dropping {what} panicked; `pg_test_db_sweep` removes marked leftovers"),
    }
}

async fn drop_now(admin: &PgConnectOptions, database: Option<&str>, roles: &[String]) -> Result<(), String> {
    let mut conn = PgConnection::connect_with(admin).await.map_err(|e| format!("connect: {e}"))?;
    if let Some(db) = database {
        conn.execute(format!("DROP DATABASE IF EXISTS \"{db}\" WITH (FORCE)").as_str())
            .await
            .map_err(|e| format!("DROP DATABASE {db}: {e}"))?;
    }
    let mut failed = Vec::new();
    for role in roles {
        if let Err(e) = conn.execute(format!("DROP ROLE IF EXISTS \"{role}\"").as_str()).await {
            failed.push(format!("DROP ROLE {role}: {e}"));
        }
    }
    conn.close().await.ok();
    if failed.is_empty() { Ok(()) } else { Err(failed.join("; ")) }
}

/// What [`sweep`] dropped and skipped.
#[derive(Debug, Default)]
pub struct SweepReport {
    pub dropped_databases: Vec<String>,
    pub dropped_roles: Vec<String>,
    /// Marked but too young, still connected, or failed to drop, with why.
    pub skipped: Vec<(String, String)>,
}

/// Drops leftovers of killed test runs on the local server `admin` points at:
/// databases and roles whose name [`unique_name`] built, created more than
/// `min_age` ago, with no session open on them. Nothing without [`MARKER`]
/// and a parseable creation time is touched.
pub async fn sweep(admin: &PgConnectOptions, min_age: std::time::Duration) -> Result<SweepReport, sqlx::Error> {
    if std::env::var(RETAIN_ENV).as_deref() == Ok("1") {
        return Ok(SweepReport::default());
    }
    assert!(
        matches!(admin.get_host(), "localhost" | "127.0.0.1" | "::1"),
        "the sweeper only runs against a local Postgres (#742)"
    );
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).expect("clock after 1970").as_secs();
    let old_enough = |name: &str| created_at(name).is_some_and(|created| now.saturating_sub(created) >= min_age.as_secs());
    let mut conn = PgConnection::connect_with(admin).await?;
    let mut report = SweepReport::default();

    let databases: Vec<(String, i64)> = sqlx::query_as(
        "SELECT d.datname::text, (SELECT count(*) FROM pg_stat_activity a WHERE a.datid=d.oid) \
         FROM pg_database d WHERE d.datname LIKE 'cstest\\_%' AND NOT d.datistemplate ORDER BY 1",
    )
    .fetch_all(&mut conn)
    .await?;
    for (name, sessions) in databases {
        if !old_enough(&name) {
            report.skipped.push((name, "younger than the minimum age, or no creation time".into()));
        } else if sessions > 0 {
            report.skipped.push((name, format!("{sessions} open session(s)")));
        } else {
            // No FORCE: a session that connected since the check makes this fail, and that is correct.
            match conn.execute(format!("DROP DATABASE IF EXISTS \"{name}\"").as_str()).await {
                Ok(_) => report.dropped_databases.push(name),
                Err(e) => report.skipped.push((name, e.to_string())),
            }
        }
    }

    let roles: Vec<(String, i64)> = sqlx::query_as(
        "SELECT r.rolname::text, (SELECT count(*) FROM pg_stat_activity a WHERE a.usesysid=r.oid) \
         FROM pg_roles r WHERE r.rolname LIKE 'cstest\\_%' ORDER BY 1",
    )
    .fetch_all(&mut conn)
    .await?;
    for (name, sessions) in roles {
        if !old_enough(&name) {
            report.skipped.push((name, "younger than the minimum age, or no creation time".into()));
        } else if sessions > 0 {
            report.skipped.push((name, format!("{sessions} open session(s)")));
        } else {
            match conn.execute(format!("DROP ROLE IF EXISTS \"{name}\"").as_str()).await {
                Ok(_) => report.dropped_roles.push(name),
                Err(e) => report.skipped.push((name, e.to_string())),
            }
        }
    }
    conn.close().await.ok();
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unique_names_carry_the_marker_and_their_creation_time() {
        let name = unique_name("cor22_execution");
        assert!(name.starts_with("cstest_cor22_execution_"), "{name}");
        let created = created_at(&name).expect("parseable creation time");
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
        assert!(now.abs_diff(created) < 5);
        assert_ne!(unique_name("x"), unique_name("x"));
        let long = unique_name(&"a".repeat(80));
        assert_eq!(long.len(), 63);
        assert!(created_at(&long).is_some());
    }

    #[test]
    fn foreign_names_have_no_creation_time() {
        for name in ["postgres", "cor22_finance_0123456789abcdef", "cstest_x", "cstest_x_zzzzzzzz_0123456789ab", "cstest_x_0123abcd_0123", "cstest_x\"_0123abcd_0123456789ab"] {
            assert_eq!(created_at(name), None, "{name}");
        }
        assert_eq!(created_at("cstest_a_b_0000000a_0123456789ab"), Some(10));
    }
}
