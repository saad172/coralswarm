//! [`TenantDbProvisioner`]: create / find / delete the per-org tenant
//! database (self-serve org signup PR A1b, audit row 4a).
//!
//! Two implementations:
//!
//! * [`NeonProvisioner`] — the Neon REST API (`projects` create, search by
//!   name, connection URI, delete). One Neon project per org, named
//!   `coralswarm-tenant-<slug>`, exactly what `onboard_tenant.sh` step 2 did
//!   with `neonctl`. Costs real money per project, which is why
//!   [`find_by_name`](TenantDbProvisioner::find_by_name) exists: a retried
//!   job finds the project it already created instead of creating a second.
//! * [`LocalPgProvisioner`] — `CREATE DATABASE` on a dev/CI Postgres. The
//!   same trait surface, so the job's tests exercise every step against a
//!   real database with no network and no cost.
//!
//! Nothing in this module logs, formats, or embeds a DSN or the Neon API key
//! in an error. [`ProvisionedDatabase`]'s `Debug` redacts the DSN.

use std::fmt;

use async_trait::async_trait;
use serde_json::Value;
use sqlx::postgres::PgConnectOptions;
use sqlx::{Connection, Executor, PgConnection};

/// What a provider handed back: its own id for the database (a Neon project
/// id, or the local database name) and a DSN that connects as the table
/// OWNER role (Neon's `neondb_owner`), which is what every production tenant
/// pool connects as today.
#[derive(Clone)]
pub struct ProvisionedDatabase {
    pub provider_id: String,
    pub dsn: String,
}

impl fmt::Debug for ProvisionedDatabase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProvisionedDatabase")
            .field("provider_id", &self.provider_id)
            .field("dsn", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteOutcome {
    Deleted,
    /// Nothing by that id exists — a retried cleanup converging.
    AlreadyGone,
}

/// Transient = retry with backoff (network, 5xx, rate limit, a database
/// still coming up). Permanent = stop and clean up (bad credentials, an
/// invalid name, a 4xx the caller cannot fix by waiting). Messages must be
/// human-readable and must never carry a DSN or key.
#[derive(Debug, thiserror::Error)]
pub enum ProvisionerError {
    #[error("{0}")]
    Transient(String),
    #[error("{0}")]
    Permanent(String),
    /// `create` found the name already taken: something else made that
    /// database between the job's lookup and its create. Never this job's.
    #[error("{0}")]
    AlreadyExists(String),
}

#[async_trait]
pub trait TenantDbProvisioner: Send + Sync {
    /// Stable provider label recorded on the job (`neon`, `local`).
    fn provider_name(&self) -> &'static str;

    /// The provider-side name for a tenant slug: Neon project
    /// `coralswarm-tenant-<slug>` (what `onboard_tenant.sh` named them), local
    /// database `coralswarm_tenant_<slug_>`.
    fn database_name(&self, slug: &str) -> String;

    /// Create a database named `name`. Must NOT be called when
    /// [`find_by_name`](Self::find_by_name) would have found one — the job
    /// always asks first. A provider that enforces unique names returns
    /// [`ProvisionerError::AlreadyExists`] rather than converging on a
    /// database it did not just create.
    async fn create(
        &self,
        name: &str,
        region: Option<&str>,
    ) -> Result<ProvisionedDatabase, ProvisionerError>;

    /// The database previously created under `name`, if any, with a DSN that
    /// still works. `Ok(None)` when none exists. More than one database by
    /// that name is a permanent error: there is no telling which is whose.
    async fn find_by_name(&self, name: &str) -> Result<Option<ProvisionedDatabase>, ProvisionerError>;

    /// An owner DSN for the database with this provider id, `Ok(None)` when
    /// it no longer exists. Cleanup reads the fingerprint through it before
    /// deleting anything.
    async fn connection_dsn(&self, provider_id: &str) -> Result<Option<String>, ProvisionerError>;

    /// The provider's own name for the database with this id (a Neon
    /// project's `name`, a local database's `datname`), `Ok(None)` when it no
    /// longer exists. Cleanup requires it to equal
    /// [`database_name`](Self::database_name)`(job.slug)` before deleting, so
    /// a recorded id that points at somebody else's database is refused.
    async fn name_of(&self, provider_id: &str) -> Result<Option<String>, ProvisionerError>;

    /// Whether `name` is a SELF-SERVE tenant name on this provider
    /// ([`database_name`](Self::database_name) of an `o-…` slug). Cleanup
    /// never deletes anything else, whatever the job row says.
    fn is_selfserve_name(&self, name: &str) -> bool;

    /// Delete by provider id. Idempotent.
    async fn delete(&self, provider_id: &str) -> Result<DeleteOutcome, ProvisionerError>;
}

// ── Local (dev / CI) ──────────────────────────────────────────────────────────

/// `CREATE DATABASE` on the server `admin_dsn` points at, connecting as that
/// same (owner) role. The returned DSN is `admin_dsn` with the database
/// swapped, so the "owner" the tenant pool connects as is the dev/CI role.
pub struct LocalPgProvisioner {
    admin_dsn: String,
}

impl LocalPgProvisioner {
    pub fn new(admin_dsn: impl Into<String>) -> Self {
        LocalPgProvisioner {
            admin_dsn: admin_dsn.into(),
        }
    }

    fn valid_name(name: &str) -> Result<(), ProvisionerError> {
        let ok = !name.is_empty()
            && name.len() <= 63
            && name
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
        if ok {
            Ok(())
        } else {
            Err(ProvisionerError::Permanent(format!(
                "local provisioner: {name:?} is not a safe database name"
            )))
        }
    }

    fn dsn_for(&self, name: &str) -> Result<String, ProvisionerError> {
        let opts: PgConnectOptions = self
            .admin_dsn
            .parse()
            .map_err(|_| ProvisionerError::Permanent("local provisioner: admin DSN is invalid".into()))?;
        let user = opts.get_username();
        let host = opts.get_host();
        let port = opts.get_port();
        // Password (if any) is carried verbatim from the admin DSN's userinfo
        // rather than re-derived — never printed.
        let userinfo = match self.admin_dsn.split("://").nth(1).and_then(|r| r.split('@').next()) {
            Some(u) if self.admin_dsn.contains('@') => u.to_string(),
            _ => user.to_string(),
        };
        Ok(format!("postgres://{userinfo}@{host}:{port}/{name}"))
    }

    async fn admin(&self) -> Result<PgConnection, ProvisionerError> {
        PgConnection::connect(&self.admin_dsn)
            .await
            .map_err(|e| ProvisionerError::Transient(format!("local provisioner: connect: {e}")))
    }
}

#[async_trait]
impl TenantDbProvisioner for LocalPgProvisioner {
    fn provider_name(&self) -> &'static str {
        "local"
    }

    fn database_name(&self, slug: &str) -> String {
        format!("coralswarm_tenant_{}", slug.replace('-', "_"))
    }

    async fn create(
        &self,
        name: &str,
        _region: Option<&str>,
    ) -> Result<ProvisionedDatabase, ProvisionerError> {
        Self::valid_name(name)?;
        let mut conn = self.admin().await?;
        match conn.execute(format!("CREATE DATABASE \"{name}\"").as_str()).await {
            Ok(_) => {}
            // 42P04 duplicate_database: the job looked it up first and found
            // nothing, so someone else created it in between.
            Err(sqlx::Error::Database(e)) if e.code().as_deref() == Some("42P04") => {
                return Err(ProvisionerError::AlreadyExists(format!(
                    "local provisioner: database {name} already exists and was not created by this call"
                )))
            }
            Err(e) => {
                return Err(ProvisionerError::Transient(format!(
                    "local provisioner: CREATE DATABASE: {e}"
                )))
            }
        }
        Ok(ProvisionedDatabase {
            provider_id: name.to_string(),
            dsn: self.dsn_for(name)?,
        })
    }

    async fn find_by_name(&self, name: &str) -> Result<Option<ProvisionedDatabase>, ProvisionerError> {
        Self::valid_name(name)?;
        let mut conn = self.admin().await?;
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)")
                .bind(name)
                .fetch_one(&mut conn)
                .await
                .map_err(|e| ProvisionerError::Transient(format!("local provisioner: pg_database: {e}")))?;
        Ok(if exists {
            Some(ProvisionedDatabase {
                provider_id: name.to_string(),
                dsn: self.dsn_for(name)?,
            })
        } else {
            None
        })
    }

    async fn connection_dsn(&self, provider_id: &str) -> Result<Option<String>, ProvisionerError> {
        Ok(self.find_by_name(provider_id).await?.map(|db| db.dsn))
    }

    async fn name_of(&self, provider_id: &str) -> Result<Option<String>, ProvisionerError> {
        // A local database's id IS its name.
        Ok(self.find_by_name(provider_id).await?.map(|db| db.provider_id))
    }

    fn is_selfserve_name(&self, name: &str) -> bool {
        name.starts_with("coralswarm_tenant_o_")
    }

    async fn delete(&self, provider_id: &str) -> Result<DeleteOutcome, ProvisionerError> {
        Self::valid_name(provider_id)?;
        let mut conn = self.admin().await?;
        let existed: bool =
            sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)")
                .bind(provider_id)
                .fetch_one(&mut conn)
                .await
                .map_err(|e| ProvisionerError::Transient(format!("local provisioner: pg_database: {e}")))?;
        conn.execute(format!("DROP DATABASE IF EXISTS \"{provider_id}\" WITH (FORCE)").as_str())
            .await
            .map_err(|e| ProvisionerError::Transient(format!("local provisioner: DROP DATABASE: {e}")))?;
        Ok(if existed {
            DeleteOutcome::Deleted
        } else {
            DeleteOutcome::AlreadyGone
        })
    }
}

// ── Neon ──────────────────────────────────────────────────────────────────────

pub const NEON_API_BASE: &str = "https://console.neon.tech/api/v2";
/// Neon's defaults for a fresh project. The create response names them too;
/// `find_by_name` has to ask for a URI by role/database and uses these.
const NEON_DEFAULT_DATABASE: &str = "neondb";
const NEON_DEFAULT_ROLE: &str = "neondb_owner";

/// Neon REST implementation. `pg_version` is pinned (the tenant tree is
/// exercised against a single major in CI); `default_region` is used when the
/// job carries none (Neon picks its own default when both are absent).
pub struct NeonProvisioner {
    http: reqwest::Client,
    api_key: String,
    base_url: String,
    pg_version: u8,
    default_region: Option<String>,
    name_prefix: String,
}

impl NeonProvisioner {
    pub fn new(api_key: impl Into<String>) -> Self {
        NeonProvisioner {
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(60))
                .build()
                .expect("reqwest client"),
            api_key: api_key.into(),
            base_url: NEON_API_BASE.to_string(),
            pg_version: 17,
            default_region: None,
            name_prefix: "coralswarm-tenant-".into(),
        }
    }

    /// Project-name prefix. Production keeps the default; the live check
    /// uses `selfserve-test-` so every project it creates is named per the
    /// test-resource rule and never looks like a real tenant.
    pub fn with_name_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.name_prefix = prefix.into();
        self
    }

    /// `NEON_API_KEY` (required) and `NEON_DEFAULT_REGION` (optional).
    pub fn from_env() -> Option<Self> {
        let key = std::env::var("NEON_API_KEY").ok()?;
        if key.trim().is_empty() {
            return None;
        }
        let mut p = NeonProvisioner::new(key);
        p.default_region = std::env::var("NEON_DEFAULT_REGION")
            .ok()
            .filter(|r| !r.trim().is_empty());
        Some(p)
    }

    pub fn with_base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = url.into();
        self
    }

    pub fn with_pg_version(mut self, v: u8) -> Self {
        self.pg_version = v;
        self
    }

    async fn send(&self, req: reqwest::RequestBuilder, what: &str) -> Result<Value, ProvisionerError> {
        let resp = req
            .bearer_auth(&self.api_key)
            .header("accept", "application/json")
            .send()
            .await
            .map_err(|e| ProvisionerError::Transient(format!("neon {what}: transport: {e}")))?;
        let status = resp.status();
        let body: Value = resp.json().await.unwrap_or(Value::Null);
        if status.is_success() {
            return Ok(body);
        }
        let message = body
            .get("message")
            .and_then(|m| m.as_str())
            .unwrap_or("no message")
            .to_string();
        let code = status.as_u16();
        if status.is_server_error() || code == 429 || code == 423 {
            Err(ProvisionerError::Transient(format!("neon {what}: HTTP {code}: {message}")))
        } else {
            Err(ProvisionerError::Permanent(format!("neon {what}: HTTP {code}: {message}")))
        }
    }

    /// `GET /projects/{id}/connection_uri?pooled=true` — the pooled DSN, which
    /// is what production tenant pools connect through
    /// (`tenancy::migrate` derives the direct endpoint for session locks).
    async fn connection_uri(&self, project_id: &str) -> Result<String, ProvisionerError> {
        let url = format!("{}/projects/{project_id}/connection_uri", self.base_url);
        let v = self
            .send(
                self.http.get(&url).query(&[
                    ("database_name", NEON_DEFAULT_DATABASE),
                    ("role_name", NEON_DEFAULT_ROLE),
                    ("pooled", "true"),
                ]),
                "connection_uri",
            )
            .await?;
        v.get("uri")
            .and_then(|u| u.as_str())
            .map(str::to_string)
            .ok_or_else(|| ProvisionerError::Permanent("neon connection_uri: response carried no uri".into()))
    }
}

/// A Neon project id as the API returns it (`[a-z0-9-]`, e.g.
/// `cool-meadow-12345678`). Every URL this provisioner builds from a
/// recorded id goes through this first, so a tampered id can never become a
/// different path (`../`, a query string) on the delete endpoint.
fn valid_project_id(id: &str) -> Result<(), ProvisionerError> {
    let ok = !id.is_empty()
        && id.len() <= 63
        && id.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
    if ok {
        Ok(())
    } else {
        Err(ProvisionerError::Permanent(format!("neon: {id:?} is not a Neon project id")))
    }
}

/// Swap the host of a Neon connection URI for its pooler host, leaving
/// userinfo, database and query untouched. Returns the input unchanged when
/// no pooler host is given.
fn pooled_uri(uri: &str, pooler_host: Option<&str>) -> String {
    let Some(pooler) = pooler_host.filter(|h| !h.is_empty()) else {
        return uri.to_string();
    };
    let Some(at) = uri.rfind('@') else {
        return uri.to_string();
    };
    let (prefix, rest) = uri.split_at(at + 1);
    let host_end = rest.find(['/', '?', ':']).unwrap_or(rest.len());
    format!("{prefix}{pooler}{}", &rest[host_end..])
}

/// The id of the one project in a `projects.list` response named exactly
/// `name` (`search` is a substring match). Neon does not enforce unique
/// project names, so two exact matches are refused rather than guessed at.
fn exact_project_id(list: &Value, name: &str) -> Result<Option<String>, ProvisionerError> {
    let matches: Vec<&str> = list
        .get("projects")
        .and_then(|p| p.as_array())
        .map(|projects| {
            projects
                .iter()
                .filter(|p| p.get("name").and_then(|n| n.as_str()) == Some(name))
                .filter_map(|p| p.get("id").and_then(|i| i.as_str()))
                .collect()
        })
        .unwrap_or_default();
    match matches.as_slice() {
        [] => Ok(None),
        [one] => Ok(Some(one.to_string())),
        many => Err(ProvisionerError::Permanent(format!(
            "neon projects.list: {} projects are named {name:?} ({}); refusing to pick one",
            many.len(),
            many.join(", ")
        ))),
    }
}

#[async_trait]
impl TenantDbProvisioner for NeonProvisioner {
    fn provider_name(&self) -> &'static str {
        "neon"
    }

    fn database_name(&self, slug: &str) -> String {
        format!("{}{slug}", self.name_prefix)
    }

    async fn create(
        &self,
        name: &str,
        region: Option<&str>,
    ) -> Result<ProvisionedDatabase, ProvisionerError> {
        let url = format!("{}/projects", self.base_url);
        let mut project = serde_json::json!({ "name": name, "pg_version": self.pg_version });
        if let Some(r) = region.or(self.default_region.as_deref()) {
            project["region_id"] = Value::String(r.to_string());
        }
        let v = self
            .send(
                self.http.post(&url).json(&serde_json::json!({ "project": project })),
                "projects.create",
            )
            .await?;
        let id = v
            .pointer("/project/id")
            .and_then(|i| i.as_str())
            .map(str::to_string)
            .ok_or_else(|| ProvisionerError::Permanent("neon projects.create: response carried no project id".into()))?;
        valid_project_id(&id)?;
        let first = v.pointer("/connection_uris/0");
        let uri = first
            .and_then(|c| c.get("connection_uri"))
            .and_then(|u| u.as_str())
            .map(str::to_string);
        let pooler_host = first
            .and_then(|c| c.pointer("/connection_parameters/pooler_host"))
            .and_then(|h| h.as_str())
            .map(str::to_string);
        let dsn = match uri {
            Some(u) => pooled_uri(&u, pooler_host.as_deref()),
            // The project exists; its id is what matters for idempotency.
            // Ask for the URI the same way a retry would.
            None => self.connection_uri(&id).await?,
        };
        Ok(ProvisionedDatabase {
            provider_id: id,
            dsn,
        })
    }

    async fn find_by_name(&self, name: &str) -> Result<Option<ProvisionedDatabase>, ProvisionerError> {
        let url = format!("{}/projects", self.base_url);
        let v = self
            .send(
                self.http.get(&url).query(&[("search", name), ("limit", "20")]),
                "projects.list",
            )
            .await?;
        let Some(id) = exact_project_id(&v, name)? else {
            return Ok(None);
        };
        valid_project_id(&id)?;
        let dsn = self.connection_uri(&id).await?;
        Ok(Some(ProvisionedDatabase {
            provider_id: id,
            dsn,
        }))
    }

    async fn connection_dsn(&self, provider_id: &str) -> Result<Option<String>, ProvisionerError> {
        valid_project_id(provider_id)?;
        match self.connection_uri(provider_id).await {
            Ok(uri) => Ok(Some(uri)),
            Err(ProvisionerError::Permanent(m)) if m.contains("HTTP 404") => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// `GET /projects/{id}` → `project.name`.
    async fn name_of(&self, provider_id: &str) -> Result<Option<String>, ProvisionerError> {
        valid_project_id(provider_id)?;
        let url = format!("{}/projects/{provider_id}", self.base_url);
        match self.send(self.http.get(&url), "projects.get").await {
            Ok(v) => v
                .pointer("/project/name")
                .and_then(|n| n.as_str())
                .map(|n| Some(n.to_string()))
                .ok_or_else(|| ProvisionerError::Permanent("neon projects.get: response carried no project name".into())),
            Err(ProvisionerError::Permanent(m)) if m.contains("HTTP 404") => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn is_selfserve_name(&self, name: &str) -> bool {
        name.starts_with(&format!("{}o-", self.name_prefix))
    }

    /// Callers delete only after [`name_of`](TenantDbProvisioner::name_of)
    /// matched this job's name; the id is still validated here so no path
    /// but `/projects/<id>` is ever sent a DELETE.
    async fn delete(&self, provider_id: &str) -> Result<DeleteOutcome, ProvisionerError> {
        valid_project_id(provider_id)?;
        let url = format!("{}/projects/{provider_id}", self.base_url);
        match self.send(self.http.delete(&url), "projects.delete").await {
            Ok(_) => Ok(DeleteOutcome::Deleted),
            Err(ProvisionerError::Permanent(m)) if m.contains("HTTP 404") => Ok(DeleteOutcome::AlreadyGone),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pooled_uri_swaps_only_the_host() {
        let uri = "postgresql://neondb_owner:p%40ss@ep-cool-1.us-east-2.aws.neon.tech/neondb?sslmode=require";
        assert_eq!(
            pooled_uri(uri, Some("ep-cool-1-pooler.us-east-2.aws.neon.tech")),
            "postgresql://neondb_owner:p%40ss@ep-cool-1-pooler.us-east-2.aws.neon.tech/neondb?sslmode=require"
        );
        assert_eq!(pooled_uri(uri, None), uri);
        let with_port = "postgresql://u:p@host:5432/db";
        assert_eq!(pooled_uri(with_port, Some("pooler")), "postgresql://u:p@pooler:5432/db");
    }

    #[test]
    fn neon_find_matches_the_exact_name_once_or_refuses() {
        let list = serde_json::json!({ "projects": [
            { "id": "p-sub", "name": "coralswarm-tenant-acme-corp" },
            { "id": "p-1", "name": "coralswarm-tenant-acme" },
        ]});
        assert_eq!(exact_project_id(&list, "coralswarm-tenant-acme").unwrap().as_deref(), Some("p-1"));
        assert_eq!(exact_project_id(&list, "coralswarm-tenant-zzz").unwrap(), None);
        let dup = serde_json::json!({ "projects": [
            { "id": "p-1", "name": "coralswarm-tenant-acme" },
            { "id": "p-2", "name": "coralswarm-tenant-acme" },
        ]});
        let e = exact_project_id(&dup, "coralswarm-tenant-acme").unwrap_err();
        assert!(matches!(&e, ProvisionerError::Permanent(m) if m.contains("p-1") && m.contains("p-2")), "{e:?}");
    }

    #[test]
    fn neon_project_ids_are_validated_before_any_url_is_built() {
        assert!(valid_project_id("cool-meadow-12345678").is_ok());
        for bad in ["", "../projects", "abc/def", "abc?x=1", "ABC", "a b", "x".repeat(64).as_str()] {
            assert!(valid_project_id(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn only_selfserve_names_are_selfserve() {
        let neon = NeonProvisioner::new("k");
        assert!(neon.is_selfserve_name(&neon.database_name("o-0123456789abcdef")));
        assert!(!neon.is_selfserve_name("coralswarm-tenant-acme-corp"));
        assert!(!neon.is_selfserve_name("some-other-project"));
        let test = NeonProvisioner::new("k").with_name_prefix("selfserve-test-");
        assert!(test.is_selfserve_name(&test.database_name("o-0123456789abcdef")));
        assert!(!test.is_selfserve_name("coralswarm-tenant-o-0123456789abcdef"));
        let local = LocalPgProvisioner::new("postgres://x@localhost/postgres");
        assert!(local.is_selfserve_name(&local.database_name("o-0123456789abcdef")));
        assert!(!local.is_selfserve_name("coralswarm_tenant_acme"));
    }

    #[test]
    fn provisioned_database_debug_redacts_the_dsn() {
        let p = ProvisionedDatabase {
            provider_id: "proj".into(),
            dsn: "postgres://u:hunter2@h/db".into(),
        };
        let s = format!("{p:?}");
        assert!(s.contains("proj"));
        assert!(!s.contains("hunter2"));
    }

    #[test]
    fn local_names_are_validated() {
        assert!(LocalPgProvisioner::valid_name("coralswarm_tenant_acme").is_ok());
        assert!(LocalPgProvisioner::valid_name("Acme").is_err());
        assert!(LocalPgProvisioner::valid_name("x; DROP").is_err());
    }

    #[test]
    fn local_dsn_keeps_userinfo_and_swaps_database() {
        let p = LocalPgProvisioner::new("postgres://alice:s3cret@localhost:5433/postgres");
        assert_eq!(
            p.dsn_for("coralswarm_tenant_x").unwrap(),
            "postgres://alice:s3cret@localhost:5433/coralswarm_tenant_x"
        );
        let p = LocalPgProvisioner::new("postgres://alice@localhost:5432/postgres");
        assert_eq!(p.dsn_for("t").unwrap(), "postgres://alice@localhost:5432/t");
    }
}
