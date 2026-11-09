//! `library_listing_switch` — read or flip ONE plane's Library listing switch.
//!
//! The `library_listing_switch` row of each plane (an org tenant database, or
//! the control database for consumer accounts) is the only source of truth
//! for the listing program and its capture triggers (tenant 0202 / control
//! 0383). No backend instance writes it from its environment; this command
//! is the explicit operator action that flips it. `LIBRARY_LISTING_FACTS`
//! is ignored (with a warning at backend startup).
//!
//! A flip commits in its own transaction, then stamps the generation's flip
//! barrier in a second one (`flip_listing_switch`). The worker starts no
//! rebuild for the new generation until every transaction that could have
//! read the switch as Off has ended; `status` reports that as `settled`.
//! Every flip to On is a new generation: every graph on the plane rebuilds
//! once. Flip only after every backend instance runs a build with this
//! command: an older instance whose env disagrees with the row has its
//! per-tick mirror refused (its library_evidence tick logs the error), but
//! it is the deploy, not this command, that ends that.
//!
//! ## Usage
//!
//!   LIBRARY_SWITCH_DATABASE_URL=postgres://… library_listing_switch \
//!     status|on|off --plane tenant|control [--org ORG_ID] [--dsn-env VAR]
//!
//! `--plane` must name the database the DSN reaches (a tenant database has
//! `tenant_meta`); `on`/`off` on a tenant plane also require `--org` to equal
//! its `tenant_meta.org_id`, so a DSN pasted for the wrong org flips nothing.
//! The DSN is read from the env var (default `LIBRARY_SWITCH_DATABASE_URL`)
//! and never printed. Use the direct endpoint or a pooler: every setting is
//! transaction-local. Output is one JSON line.
//!
//! ## Exit codes
//!
//!   0  printed (a no-op flip is still 0, with `"flipped":false`)
//!   1  database error
//!   2  usage error, the DSN env var is unset, or the plane/org check failed

use std::process::ExitCode;

use chrono::{DateTime, Utc};
use serde_json::json;
use sqlx::{Connection, PgConnection};

use coralswarm_backend::workers::library_evidence as worker;

const USAGE: &str = "usage: library_listing_switch status|on|off --plane tenant|control [--org ORG_ID] [--dsn-env VAR]";

#[derive(Debug, PartialEq, Eq)]
enum Action {
    Status,
    Flip(bool),
}

#[derive(Debug, PartialEq, Eq)]
struct Args {
    action: Action,
    tenant: bool,
    org: Option<String>,
    dsn_env: String,
}

fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Args, String> {
    let action = match args.next().as_deref() {
        Some("status") => Action::Status,
        Some("on") => Action::Flip(true),
        Some("off") => Action::Flip(false),
        _ => return Err(USAGE.into()),
    };
    let (mut tenant, mut org, mut dsn_env) = (None, None, "LIBRARY_SWITCH_DATABASE_URL".to_owned());
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--plane" => match args.next().as_deref() {
                Some("tenant") => tenant = Some(true),
                Some("control") => tenant = Some(false),
                _ => return Err("--plane must be tenant or control".into()),
            },
            "--org" => org = Some(args.next().ok_or("--org requires a value")?),
            "--dsn-env" => dsn_env = args.next().ok_or("--dsn-env requires a value")?,
            _ => return Err(format!("unknown argument {arg:?}\n{USAGE}")),
        }
    }
    let tenant = tenant.ok_or("--plane is required")?;
    if !tenant && org.is_some() {
        return Err("--org names a tenant plane; the control plane has none".into());
    }
    if tenant && matches!(action, Action::Flip(_)) && org.is_none() {
        return Err("flipping a tenant plane requires --org".into());
    }
    Ok(Args { action, tenant, org, dsn_env })
}

#[derive(sqlx::FromRow)]
struct Row {
    enabled: bool,
    enabled_since: Option<DateTime<Utc>>,
    barrier_stamped: bool,
}

async fn row(conn: &mut PgConnection) -> Result<serde_json::Value, sqlx::Error> {
    let r: Row = sqlx::query_as("SELECT enabled,enabled_since,(barrier_xid IS NOT NULL AND barrier_since IS NOT DISTINCT FROM enabled_since) barrier_stamped FROM library_listing_switch")
        .fetch_one(&mut *conn)
        .await?;
    let settled = worker::listing_switch_settled(&mut *conn).await?;
    Ok(json!({"enabled":r.enabled,"enabled_since":r.enabled_since,"barrier_stamped":r.enabled && r.barrier_stamped,"settled":settled}))
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let args = match parse_args(std::env::args().skip(1)) {
        Ok(args) => args,
        Err(message) => {
            eprintln!("{message}");
            return ExitCode::from(2);
        }
    };
    let Ok(dsn) = std::env::var(&args.dsn_env) else {
        eprintln!("{} is not set", args.dsn_env);
        return ExitCode::from(2);
    };
    let mut conn = match tokio::time::timeout(std::time::Duration::from_secs(10), PgConnection::connect(&dsn)).await {
        Ok(Ok(conn)) => conn,
        // The connect error can echo DSN parts; print the category only.
        Ok(Err(_)) | Err(_) => {
            eprintln!("{}", json!({"complete":false,"error_category":"database_connect"}));
            return ExitCode::from(1);
        }
    };
    match run(&mut conn, &args).await {
        Ok(Ok(out)) => {
            println!("{out}");
            ExitCode::SUCCESS
        }
        Ok(Err(check)) => {
            eprintln!("{}", json!({"complete":false,"error_category":check}));
            ExitCode::from(2)
        }
        Err(e) => {
            eprintln!("{}", json!({"complete":false,"error_category":"database","error":e.to_string()}));
            ExitCode::from(1)
        }
    }
}

async fn run(conn: &mut PgConnection, args: &Args) -> Result<Result<serde_json::Value, &'static str>, sqlx::Error> {
    let is_tenant: bool = sqlx::query_scalar("SELECT to_regclass('public.tenant_meta') IS NOT NULL")
        .fetch_one(&mut *conn)
        .await?;
    if is_tenant != args.tenant {
        return Ok(Err("plane_mismatch"));
    }
    let org: Option<String> = if is_tenant {
        Some(sqlx::query_scalar("SELECT org_id FROM tenant_meta WHERE id").fetch_one(&mut *conn).await?)
    } else {
        None
    };
    if args.org.is_some() && args.org != org {
        return Ok(Err("org_mismatch"));
    }
    let plane = if is_tenant { "tenant" } else { "control" };
    let before = row(&mut *conn).await?;
    let out = match args.action {
        Action::Status => json!({"plane":plane,"org_id":org,"switch":before}),
        Action::Flip(on) => {
            let flipped = worker::flip_listing_switch(&mut *conn, on).await?;
            let after = row(&mut *conn).await?;
            json!({"plane":plane,"org_id":org,"requested":if on {"on"} else {"off"},"flipped":flipped.is_some(),"before":before,"after":after})
        }
    };
    Ok(Ok(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn parse(s: &str) -> Result<Args, String> {
        parse_args(s.split_whitespace().map(str::to_owned))
    }
    #[test]
    fn flips_name_their_plane_and_a_tenant_flip_names_its_org() {
        assert_eq!(parse("on --plane tenant --org org_1").unwrap(), Args { action: Action::Flip(true), tenant: true, org: Some("org_1".into()), dsn_env: "LIBRARY_SWITCH_DATABASE_URL".into() });
        assert_eq!(parse("off --plane control --dsn-env X").unwrap().dsn_env, "X");
        assert_eq!(parse("status --plane tenant").unwrap().action, Action::Status);
        assert!(parse("on --plane tenant").is_err(), "a tenant flip without --org");
        assert!(parse("on").is_err(), "no plane");
        assert!(parse("off --plane control --org org_1").is_err());
        assert!(parse("enable --plane control").is_err());
        assert!(parse("").is_err());
    }
}
