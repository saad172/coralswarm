//! A real LOGIN for `coralswarm_provisioner` (control 0381), the role the
//! self-serve provisioning worker writes through in production. Mirrors the
//! operator steps in PR #1517: a separate login role that is GRANTed the
//! NOLOGIN group role, never the group role itself made LOGIN.
//!
//! Roles are cluster-wide and the suites build many scratch databases
//! concurrently, so creation and the password are retried on the same
//! shared-catalog SQLSTATEs as [`super::app_role`].

use sqlx::PgPool;

/// The login role the suites connect as.
pub const PROVISIONER_TEST_LOGIN: &str = "coralswarm_provisioner_test";

/// Test-only password (scratch databases, not a secret).
pub const PROVISIONER_TEST_PASSWORD: &str = "orgt_provisioner_role_test_only_pw";

/// Create (if absent) the test login, set its password, and grant it
/// `coralswarm_provisioner`. `owner` must be a pool on a database already at
/// the control tree's head (0381 created the group role).
pub async fn prepare_provisioner_login(owner: &PgPool) {
    let statements = [
        format!(
            "DO $$ BEGIN \
               IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = '{PROVISIONER_TEST_LOGIN}') THEN \
                 CREATE ROLE {PROVISIONER_TEST_LOGIN} LOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOREPLICATION; \
               END IF; \
             END $$"
        ),
        format!("ALTER ROLE {PROVISIONER_TEST_LOGIN} WITH PASSWORD '{PROVISIONER_TEST_PASSWORD}'"),
        format!("GRANT coralswarm_provisioner TO {PROVISIONER_TEST_LOGIN}"),
    ];
    for sql in &statements {
        let mut attempt: u32 = 0;
        loop {
            match sqlx::query(sql).execute(owner).await {
                Ok(_) => break,
                Err(sqlx::Error::Database(e))
                    if matches!(e.code().as_deref(), Some("23505") | Some("42710") | Some("XX000")) =>
                {
                    attempt += 1;
                    assert!(attempt < 10, "prepare provisioner login: catalog race persisted: {e}");
                    let jitter = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.subsec_nanos() as u64 % 20)
                        .unwrap_or(0);
                    tokio::time::sleep(std::time::Duration::from_millis(attempt as u64 * 20 + jitter)).await;
                }
                Err(e) => panic!("prepare provisioner login ({sql}): {e}"),
            }
        }
    }
}
