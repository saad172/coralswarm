//! `org_deletion_admin`: the operator's side of in-app org deletion
//! (control 0387, `services::org_deletion`).
//!
//! ```text
//! org_deletion_admin list
//! org_deletion_admin approve <deletion-id> --operator <name>
//! org_deletion_admin cancel  <deletion-id> --operator <name>
//! org_deletion_admin export  <deletion-id> --out <file>
//! ```
//!
//! * `approve` is the required sign-off before anything is destroyed, for
//!   the first `ORG_DELETION_APPROVAL_FIRST_N` (default 10) completed prod
//!   deletions. It must run on an OWNER-level DSN (`MIGRATOR_DATABASE_URL`):
//!   0387's `org_deletions_guard_approval` trigger refuses any role that does
//!   not own the table, so the serving role can never approve its own
//!   deletion. Approving during grace is allowed (destruction still waits for
//!   grace to end).
//! * `cancel` undoes a deletion in `grace` or `awaiting_approval` (the owner
//!   can only undo during grace). Access is restored; the subscription stays
//!   cancelled. For an unpaid-origin deletion it also prints that the org
//!   re-enters deletion after 7 days if it is still unpaid, and the date.
//! * `export` writes the 30-day export (the founder's download after grace)
//!   to `--out`, created 0600. Needs `MASTER_KEK_HEX` and the R2
//!   credentials; fails once the key was destroyed at day 30. The file is
//!   decrypted customer data: handle it accordingly.
//!
//! DSN: `MIGRATOR_DATABASE_URL`, else `DATABASE_URL` (enough for `list`,
//! `cancel` and `export`, not for `approve`).

use std::process::ExitCode;
use std::sync::Arc;

use coralswarm_backend::services::org_deletion::{self, OrgDeletion};
use coralswarm_backend::services::org_deletion_live::R2ExportStore;
use coralswarm_core::r2::R2Client;
use uuid::Uuid;

fn usage() -> ExitCode {
    eprintln!(
        "usage:\n  org_deletion_admin list\n  org_deletion_admin approve <id> --operator <name>\n  \
         org_deletion_admin cancel <id> --operator <name>\n  org_deletion_admin export <id> --out <file>"
    );
    ExitCode::from(2)
}

/// `true` / `false` / `-` (not yet decided), not Rust `Debug` output.
fn approval_label(v: Option<bool>) -> &'static str {
    match v {
        Some(true) => "true",
        Some(false) => "false",
        None => "-",
    }
}

fn flag(args: &[String], name: &str) -> Option<String> {
    args.iter().position(|a| a == name).and_then(|i| args.get(i + 1)).cloned()
}

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(cmd) = args.first().map(String::as_str) else { return usage() };
    let Some(url) = ["MIGRATOR_DATABASE_URL", "DATABASE_URL"]
        .iter()
        .find_map(|k| std::env::var(k).ok().filter(|v| !v.trim().is_empty()))
    else {
        eprintln!("ERROR: set MIGRATOR_DATABASE_URL (or DATABASE_URL) to the control-plane DSN");
        return ExitCode::FAILURE;
    };
    let pool = match sqlx::postgres::PgPoolOptions::new().max_connections(2).connect(&url).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("ERROR: connect: {e}");
            return ExitCode::FAILURE;
        }
    };
    let id = || args.get(1).and_then(|s| Uuid::parse_str(s).ok());
    match cmd {
        "list" => {
            let rows: Vec<OrgDeletion> = match sqlx::query_as(
                "SELECT * FROM org_deletions WHERE state NOT IN ('done', 'canceled') ORDER BY created_at",
            )
            .fetch_all(&pool)
            .await
            {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("ERROR: {e}");
                    return ExitCode::FAILURE;
                }
            };
            for d in rows {
                println!(
                    "{}  {}  {:<18} initiated_by={:<15} grace_ends={} approval_required={} approved_by={} last_error={}",
                    d.id,
                    d.clerk_org_id,
                    d.state,
                    d.initiated_by,
                    d.grace_ends_at.format("%Y-%m-%d %H:%M"),
                    approval_label(d.approval_required),
                    d.approved_by.as_deref().unwrap_or("-"),
                    d.last_error.as_deref().unwrap_or("-"),
                );
            }
            ExitCode::SUCCESS
        }
        "approve" | "cancel" => {
            let (Some(id), Some(op)) = (id(), flag(&args, "--operator")) else { return usage() };
            let answer = if cmd == "approve" {
                org_deletion::approve(&pool, id, &op).await.map(|s| s.to_string())
            } else {
                match org_deletion::cancel_by_operator(&pool, id, &op).await {
                    Ok(r) => org_deletion::operator_cancel_message(&pool, id, r).await,
                    Err(e) => Err(e),
                }
            };
            match answer {
                Ok(a) => {
                    println!("{cmd} {id}: {a}");
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("ERROR: {e}");
                    ExitCode::FAILURE
                }
            }
        }
        "export" => {
            let (Some(id), Some(out)) = (id(), flag(&args, "--out")) else { return usage() };
            let d: OrgDeletion = match sqlx::query_as("SELECT * FROM org_deletions WHERE id = $1").bind(id).fetch_one(&pool).await {
                Ok(d) => d,
                Err(e) => {
                    eprintln!("ERROR: {e}");
                    return ExitCode::FAILURE;
                }
            };
            let (kek, next) = match (coralswarm_core::kek::load_kek(), coralswarm_core::kek::load_kek_next()) {
                (Ok(k), Ok(n)) => (k, n),
                _ => {
                    eprintln!("ERROR: MASTER_KEK_HEX (and a well-formed MASTER_KEK_HEX_NEXT, if set) is required");
                    return ExitCode::FAILURE;
                }
            };
            let r2 = match (
                std::env::var("R2_ACCOUNT_ID"),
                std::env::var("R2_ACCESS_KEY_ID"),
                std::env::var("R2_SECRET_ACCESS_KEY"),
                std::env::var("R2_BUCKET_NAME"),
            ) {
                (Ok(a), Ok(k), Ok(s), Ok(b)) => R2Client::new(&a, &k, &s, &b).await,
                _ => {
                    eprintln!("ERROR: R2_ACCOUNT_ID / R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY / R2_BUCKET_NAME are required");
                    return ExitCode::FAILURE;
                }
            };
            let store = R2ExportStore { blobs: Arc::new(r2) };
            let bytes = match org_deletion::read_export(&store, &kek, next.as_ref(), &d).await {
                Ok(b) => b,
                Err(e) => {
                    eprintln!("ERROR: {e}");
                    return ExitCode::FAILURE;
                }
            };
            if let Err(e) = write_private(&out, &bytes) {
                eprintln!("ERROR: write {out}: {e}");
                return ExitCode::FAILURE;
            }
            println!("wrote {} bytes to {out} (decrypted customer data)", bytes.len());
            ExitCode::SUCCESS
        }
        _ => usage(),
    }
}

fn write_private(path: &str, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    opts.open(path)?.write_all(bytes)
}

#[cfg(test)]
mod tests {
    use super::approval_label;

    #[test]
    fn approval_required_prints_plainly() {
        assert_eq!(approval_label(Some(true)), "true");
        assert_eq!(approval_label(Some(false)), "false");
        assert_eq!(approval_label(None), "-");
    }
}
