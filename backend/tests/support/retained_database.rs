use sqlx::{postgres::PgConnectOptions, Executor, PgConnection};
use std::str::FromStr;

enum Lifecycle {
    Recreate,
    Retain,
}

impl Lifecycle {
    fn from_env() -> Self {
        match std::env::var("ORGT_TEST_RETAIN_DATABASES") {
            Err(std::env::VarError::NotPresent) => Self::Recreate,
            Ok(value) if value == "1" => {
                let base = std::env::var("ORGT_TEST_BASE_URL")
                    .expect("retention requires ORGT_TEST_BASE_URL");
                let options = PgConnectOptions::from_str(&base)
                    .expect("retention requires a valid PostgreSQL URL");
                assert!(
                    options.get_socket().is_none()
                        && matches!(options.get_host(), "127.0.0.1" | "localhost" | "::1"),
                    "database retention is restricted to a loopback PostgreSQL host"
                );
                Self::Retain
            }
            _ => panic!("ORGT_TEST_RETAIN_DATABASES must be unset or exactly 1"),
        }
    }
}

fn identifier(db: &str) -> String {
    assert!(
        !db.is_empty() && db.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_'),
        "invalid scratch database name"
    );
    format!("\"{db}\"")
}

pub async fn recreate(conn: &mut PgConnection, db: &str) -> Result<(), sqlx::Error> {
    let name = identifier(db);
    match Lifecycle::from_env() {
        Lifecycle::Retain => {
            let exists: bool =
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)")
                    .bind(db)
                    .fetch_one(&mut *conn)
                    .await?;
            assert!(!exists, "retained scratch database already exists: {db}");
        }
        Lifecycle::Recreate => {
            conn.execute(format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)").as_str())
                .await?;
        }
    }
    conn.execute(format!("CREATE DATABASE {name}").as_str())
        .await?;
    Ok(())
}

pub async fn finish(conn: &mut PgConnection, db: &str, if_exists: bool) -> Result<(), sqlx::Error> {
    if matches!(Lifecycle::from_env(), Lifecycle::Retain) {
        return Ok(());
    }
    let name = identifier(db);
    let clause = if if_exists { "IF EXISTS " } else { "" };
    conn.execute(format!("DROP DATABASE {clause}{name} WITH (FORCE)").as_str())
        .await?;
    Ok(())
}
