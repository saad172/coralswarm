//! Resolve a secret *reference* — the value stored in
//! `tenant_databases.db_secret_ref` — to a tenant database DSN.
//!
//! The raw DSN (which carries credentials) is never stored in the routing table
//! and is never logged. Errors carry only the secret *reference name* (safe: it
//! is just an env-var / secret name), never the resolved DSN.
//!
//! Two resolution surfaces (self-serve org signup, PR A1a):
//!
//! * [`SecretResolver::resolve`] — synchronous, environment/static only.
//! * [`SecretResolver::resolve_async`] — what the server's
//!   [`TenantRegistry`](super::registry::TenantRegistry), the
//!   `migrate_tenants` orchestrator and (through [`OperatorDsnResolver`])
//!   every per-tenant operator binary under `bin/` call (issue #1514: a
//!   self-serve tenant has no Fly secret, so an env-only bin cannot reach
//!   it). Defaults to the synchronous answer;
//!   [`ChainSecretResolver`] overrides it to fall through to the KEK-encrypted
//!   `tenant_db_credentials` control table when the environment has no entry.
//!   Environment FIRST, always: an existing concierge tenant (one Fly secret
//!   per tenant) resolves byte-identically to before the table existed, and a
//!   self-serve tenant's row can never shadow an operator-set secret.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use sqlx::PgPool;

#[derive(Debug, thiserror::Error)]
pub enum SecretError {
    /// No secret is registered under this reference name.
    #[error("tenant secret ref not found: {0}")]
    NotFound(String),
    /// The control-table lookup itself failed (connection, permission). Carries
    /// the ref name and the database error, never a DSN. Distinct from
    /// `NotFound` so a flaky control plane is retried rather than read as "this
    /// tenant has no credential".
    #[error("tenant secret ref {secret_ref}: control-table lookup failed: {source}")]
    Lookup {
        secret_ref: String,
        #[source]
        source: sqlx::Error,
    },
    /// The stored ciphertext did not decrypt under the current (or next) KEK.
    /// Fail closed: a credential we cannot read is a credential we do not serve.
    #[error("tenant secret ref {0}: stored credential could not be decrypted under the configured KEK")]
    Undecryptable(String),
}

/// Resolves `db_secret_ref` → tenant DSN. Implementations must never log the DSN.
#[async_trait]
pub trait SecretResolver: Send + Sync {
    /// Synchronous resolution: environment / static sources only.
    fn resolve(&self, secret_ref: &str) -> Result<String, SecretError>;

    /// Resolution that may consult the control plane. Defaults to
    /// [`Self::resolve`]; only [`ChainSecretResolver`] does more.
    async fn resolve_async(&self, secret_ref: &str) -> Result<String, SecretError> {
        self.resolve(secret_ref)
    }
}

/// Production resolver. Fly injects each tenant DSN as an environment variable
/// named by its secret ref (e.g. `TENANT_DB_ACME_CORP`), set out-of-band by the
/// concierge onboarding script — so the routing table stores only the name.
pub struct EnvSecretResolver;

impl EnvSecretResolver {
    pub fn new() -> Self {
        EnvSecretResolver
    }
}

impl Default for EnvSecretResolver {
    fn default() -> Self {
        EnvSecretResolver::new()
    }
}

impl SecretResolver for EnvSecretResolver {
    fn resolve(&self, secret_ref: &str) -> Result<String, SecretError> {
        // Deliberately does not log the value on success or failure.
        std::env::var(secret_ref).map_err(|_| SecretError::NotFound(secret_ref.to_string()))
    }
}

/// Test resolver mapping secret ref → DSN directly, so tests need no environment
/// mutation.
pub struct StaticMapResolver {
    map: HashMap<String, String>,
}

impl StaticMapResolver {
    pub fn new(map: HashMap<String, String>) -> Self {
        StaticMapResolver { map }
    }

    /// Convenience constructor from `(ref, dsn)` pairs.
    pub fn from_pairs<I, K, V>(pairs: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        StaticMapResolver {
            map: pairs
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        }
    }
}

impl SecretResolver for StaticMapResolver {
    fn resolve(&self, secret_ref: &str) -> Result<String, SecretError> {
        self.map
            .get(secret_ref)
            .cloned()
            .ok_or_else(|| SecretError::NotFound(secret_ref.to_string()))
    }
}

/// Resolver over the `tenant_db_credentials` control table (migration
/// 0381): the KEK-encrypted successor to one-Fly-secret-per-tenant. Only ever
/// reached through [`ChainSecretResolver`], after the environment has been
/// consulted. Decryption is rotation-aware (`MASTER_KEK_HEX_NEXT` first, then
/// `MASTER_KEK_HEX`), like every other KEK unwrap site. Synchronous
/// [`SecretResolver::resolve`] is always `NotFound` here — the table needs a
/// database round trip.
pub struct ControlTableSecretResolver {
    control: PgPool,
    kek: [u8; 32],
    next_kek: Option<[u8; 32]>,
}

impl ControlTableSecretResolver {
    pub fn new(control: PgPool, kek: [u8; 32], next_kek: Option<[u8; 32]>) -> Self {
        ControlTableSecretResolver {
            control,
            kek,
            next_kek,
        }
    }
}

#[async_trait]
impl SecretResolver for ControlTableSecretResolver {
    fn resolve(&self, secret_ref: &str) -> Result<String, SecretError> {
        Err(SecretError::NotFound(secret_ref.to_string()))
    }

    async fn resolve_async(&self, secret_ref: &str) -> Result<String, SecretError> {
        let ciphertext = super::credentials::fetch_ciphertext(&self.control, secret_ref)
            .await
            .map_err(|source| SecretError::Lookup {
                secret_ref: secret_ref.to_string(),
                source,
            })?;
        let Some(bytes) = ciphertext else {
            return Err(SecretError::NotFound(secret_ref.to_string()));
        };
        super::credentials::decrypt_dsn(&self.kek, self.next_kek.as_ref(), secret_ref, &bytes)
            .map_err(|_| SecretError::Undecryptable(secret_ref.to_string()))
    }
}

/// Environment first, then the control table. The production resolver once
/// `MASTER_KEK_HEX` and a control pool are available; without either, callers
/// fall back to a bare [`EnvSecretResolver`] and nothing changes.
///
/// A `Lookup` or `Undecryptable` error from the table is returned as-is (not
/// collapsed to `NotFound`): the registry must retry a flaky control plane,
/// not conclude the tenant has no credential.
pub struct ChainSecretResolver {
    env: Arc<dyn SecretResolver>,
    table: ControlTableSecretResolver,
}

impl ChainSecretResolver {
    pub fn new(env: Arc<dyn SecretResolver>, table: ControlTableSecretResolver) -> Self {
        ChainSecretResolver { env, table }
    }

    /// The production chain: process environment, then `tenant_db_credentials`
    /// under the KEK in the environment. `None` when `MASTER_KEK_HEX` is
    /// unset or malformed (the caller logs and keeps the env-only resolver).
    pub fn from_env(control: PgPool) -> Result<Self, coralswarm_core::kek::KekError> {
        let kek = coralswarm_core::kek::load_kek()?;
        let next = coralswarm_core::kek::load_kek_next()?;
        Ok(ChainSecretResolver::new(
            Arc::new(EnvSecretResolver::new()),
            ControlTableSecretResolver::new(control, kek, next),
        ))
    }
}

#[async_trait]
impl SecretResolver for ChainSecretResolver {
    fn resolve(&self, secret_ref: &str) -> Result<String, SecretError> {
        self.env.resolve(secret_ref)
    }

    async fn resolve_async(&self, secret_ref: &str) -> Result<String, SecretError> {
        match self.env.resolve_async(secret_ref).await {
            Ok(dsn) => Ok(dsn),
            Err(SecretError::NotFound(_)) => self.table.resolve_async(secret_ref).await,
            Err(e) => Err(e),
        }
    }
}

/// `^TENANT_DB_[A-Z0-9_]+$` (at most 128 bytes): only a tenant database
/// ref, never an arbitrary environment variable name. The operator bins and
/// `migrate_tenants --resolve-dsn` run on machines holding every app secret
/// in their environment, so `CLERK_SECRET_KEY` or `MASTER_KEK_HEX` must
/// never be accepted as a "ref". Mirrors the `tenant_databases` and
/// `tenant_db_credentials` CHECK constraints (control migration 0381).
pub fn is_tenant_secret_ref(name: &str) -> bool {
    const PREFIX: &str = "TENANT_DB_";
    name.len() <= 128
        && name.len() > PREFIX.len()
        && name.starts_with(PREFIX)
        && name.bytes().all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
}

/// Why [`OperatorDsnResolver::resolve`] produced no DSN. No variant carries a
/// DSN, and [`Self::NotATenantRef`] does not echo the rejected value either:
/// a DSN pasted where a ref belongs (a mistyped `db_secret_ref`, a CLI arg)
/// must not come back out in the error.
#[derive(Debug, thiserror::Error)]
pub enum OperatorDsnError {
    #[error("refusing to resolve a value that is not a tenant secret ref (expected ^TENANT_DB_[A-Z0-9_]+$)")]
    NotATenantRef,
    /// Env-only mode (no `MASTER_KEK_HEX`): the ref is not in the
    /// environment and the encrypted table was never consulted. Distinct from
    /// [`SecretError::NotFound`] so the operator sees WHY a self-serve tenant
    /// failed instead of "not found".
    #[error(
        "tenant secret ref {0} is not in the environment, and the encrypted \
         tenant_db_credentials table was not consulted because MASTER_KEK_HEX is unavailable \
         (a self-serve tenant cannot be resolved on this run)"
    )]
    NotInEnvTableUnavailable(String),
    #[error(transparent)]
    Secret(#[from] SecretError),
}

/// The ONE way a per-tenant operator binary (and `migrate_tenants`) turns a
/// `db_secret_ref` into a DSN: [`ChainSecretResolver`] — the environment
/// FIRST (every concierge tenant's Fly secret, byte-identical), then the
/// KEK-encrypted `tenant_db_credentials` row (a self-serve tenant, AAD
/// `tenant-dsn:v1:<ref>`) — behind the [`is_tenant_secret_ref`] gate.
///
/// The control pool handed in must be a role that can SELECT
/// `tenant_db_credentials`: the owner/migrator (`MIGRATOR_DATABASE_URL`) or
/// the app role (`DATABASE_URL`), both of which hold it. This type never
/// writes and needs no grant beyond that.
///
/// Never logs. A DSN only ever leaves through `Ok`.
pub struct OperatorDsnResolver {
    inner: Arc<dyn SecretResolver>,
    /// `false` = env-only fallback (no KEK): a miss is reported as
    /// [`OperatorDsnError::NotInEnvTableUnavailable`].
    table: bool,
    /// Env-only mode's stderr line, printed once on the first resolve (so a
    /// `--control`-only run that never resolves a tenant stays quiet).
    env_only_warning: Option<String>,
    warned: std::sync::Once,
}

impl OperatorDsnResolver {
    /// Wrap an explicit resolver (tests: a [`ChainSecretResolver`] over a
    /// [`StaticMapResolver`] and a test KEK). Treated as table-backed.
    pub fn new(inner: Arc<dyn SecretResolver>) -> Self {
        OperatorDsnResolver {
            inner,
            table: true,
            env_only_warning: None,
            warned: std::sync::Once::new(),
        }
    }

    /// Strict: the production chain, or the KEK error. For callers that only
    /// reach for the table AFTER the environment missed
    /// (`migrate_tenants --resolve-dsn`), where env-only would be pointless.
    pub fn chain_from_env(control: PgPool) -> Result<Self, coralswarm_core::kek::KekError> {
        Ok(OperatorDsnResolver::new(Arc::new(ChainSecretResolver::from_env(control)?)))
    }

    /// Lenient: the production chain, or — when `MASTER_KEK_HEX` is absent
    /// or malformed (a dev box without a KEK) — the environment alone, with
    /// ONE loud stderr line naming `tool`, printed on the first resolve.
    /// Each miss then fails per tenant
    /// with [`OperatorDsnError::NotInEnvTableUnavailable`]; nothing is
    /// skipped silently.
    pub fn from_env_or_env_only(control: PgPool, tool: &str) -> Self {
        match ChainSecretResolver::from_env(control) {
            Ok(chain) => OperatorDsnResolver::new(Arc::new(chain)),
            Err(e) => OperatorDsnResolver {
                inner: Arc::new(EnvSecretResolver::new()),
                table: false,
                env_only_warning: Some(format!(
                    "{tool}: MASTER_KEK_HEX unavailable ({e}); resolving tenant DSNs from the \
                     environment ONLY. A self-serve tenant (credential in tenant_db_credentials, \
                     no Fly secret) will FAIL on this run."
                )),
                warned: std::sync::Once::new(),
            },
        }
    }

    /// The environment alone, with no control-plane connection, behind the
    /// same ref gate: `Ok(None)` = not set. `migrate_tenants --resolve-dsn`
    /// answers concierge refs from here without opening a control pool.
    pub fn resolve_from_env(secret_ref: &str) -> Result<Option<String>, OperatorDsnError> {
        if !is_tenant_secret_ref(secret_ref) {
            return Err(OperatorDsnError::NotATenantRef);
        }
        Ok(EnvSecretResolver::new().resolve(secret_ref).ok())
    }

    /// The resolver the chain wraps, for code that takes a bare
    /// [`SecretResolver`] (`tenancy::migrate::migrate_all`).
    pub fn secret_resolver(&self) -> Arc<dyn SecretResolver> {
        self.inner.clone()
    }

    /// Resolve one tenant ref: gate, environment, then the encrypted table.
    pub async fn resolve(&self, secret_ref: &str) -> Result<String, OperatorDsnError> {
        if let Some(line) = &self.env_only_warning {
            self.warned.call_once(|| eprintln!("{line}"));
        }
        if !is_tenant_secret_ref(secret_ref) {
            return Err(OperatorDsnError::NotATenantRef);
        }
        match self.inner.resolve_async(secret_ref).await {
            Ok(dsn) => Ok(dsn),
            Err(SecretError::NotFound(r)) if !self.table => {
                Err(OperatorDsnError::NotInEnvTableUnavailable(r))
            }
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tenant_secret_ref_gate_accepts_only_tenant_db_names() {
        assert!(is_tenant_secret_ref("TENANT_DB_ACME_CORP"));
        assert!(is_tenant_secret_ref("TENANT_DB_O_0123456789ABCDEF"));
        for bad in [
            "", "TENANT_DB_", "CLERK_SECRET_KEY", "MASTER_KEK_HEX", "DATABASE_URL",
            "tenant_db_acme", "TENANT_DB_acme", "TENANT_DB_A-B", "TENANT_DB_A;id", "XTENANT_DB_A",
            "postgres://u:p@h/db",
        ] {
            assert!(!is_tenant_secret_ref(bad), "{bad:?}");
        }
        assert!(!is_tenant_secret_ref(&format!("TENANT_DB_{}", "A".repeat(119))));
    }

    #[tokio::test]
    async fn operator_resolver_rejects_non_tenant_refs_without_echoing_them() {
        // The static map DOES hold these names, so only the gate can refuse.
        let leaked = "postgres://u:hunter2@h/db";
        let r = OperatorDsnResolver::new(Arc::new(StaticMapResolver::from_pairs([
            ("CLERK_SECRET_KEY", "sk_live_x"),
            (leaked, leaked),
        ])));
        for bad in ["CLERK_SECRET_KEY", leaked, "tenant_db_acme", "TENANT_DB_"] {
            let e = r.resolve(bad).await.expect_err(bad);
            assert!(matches!(e, OperatorDsnError::NotATenantRef), "{bad}: {e:?}");
            assert!(!format!("{e} {e:?}").contains("hunter2"), "{bad}");
        }
        assert!(matches!(
            OperatorDsnResolver::resolve_from_env("CLERK_SECRET_KEY"),
            Err(OperatorDsnError::NotATenantRef)
        ));
    }

    #[test]
    fn operator_resolve_from_env_reads_the_real_environment() {
        // A unique name avoids collisions with any real env in the process.
        let key = "TENANT_DB_OPR_UNIT_ENV_XYZ";
        std::env::set_var(key, "postgres://from-real-env");
        assert_eq!(
            OperatorDsnResolver::resolve_from_env(key).unwrap().as_deref(),
            Some("postgres://from-real-env")
        );
        std::env::remove_var(key);
        assert_eq!(OperatorDsnResolver::resolve_from_env(key).unwrap(), None);
    }

    #[tokio::test]
    async fn operator_resolver_env_only_miss_names_the_missing_table() {
        let r = OperatorDsnResolver {
            inner: Arc::new(StaticMapResolver::from_pairs([("TENANT_DB_A", "postgres://a")])),
            table: false,
            env_only_warning: Some("test: env-only".into()),
            warned: std::sync::Once::new(),
        };
        assert_eq!(r.resolve("TENANT_DB_A").await.unwrap(), "postgres://a");
        match r.resolve("TENANT_DB_SELF_SERVE").await {
            Err(OperatorDsnError::NotInEnvTableUnavailable(name)) => assert_eq!(name, "TENANT_DB_SELF_SERVE"),
            other => panic!("expected NotInEnvTableUnavailable, got {other:?}"),
        }
    }

    #[test]
    fn static_map_resolves_and_reports_missing() {
        let r = StaticMapResolver::from_pairs([("TENANT_DB_A", "postgres://a")]);
        assert_eq!(r.resolve("TENANT_DB_A").unwrap(), "postgres://a");
        match r.resolve("TENANT_DB_MISSING") {
            Err(SecretError::NotFound(name)) => assert_eq!(name, "TENANT_DB_MISSING"),
            other => panic!("expected NotFound, got {other:?}"),
        }
    }

    #[test]
    fn env_resolver_reads_named_var() {
        // A unique name avoids collisions with any real env in the test process.
        let key = "ORGT_TEST_SECRET_ENV_VAR_XYZ";
        std::env::set_var(key, "postgres://from-env");
        let r = EnvSecretResolver::new();
        assert_eq!(r.resolve(key).unwrap(), "postgres://from-env");
        std::env::remove_var(key);
        assert!(matches!(r.resolve(key), Err(SecretError::NotFound(_))));
    }
}
