//! `retract_fixture_kinship`: reject family-relation edges whose only
//! evidence is code text (test fixtures, doc examples). COR-66 follow-up.
//!
//! ```text
//! retract_fixture_kinship [--execute] [--tenant-dsn-env TENANT_DB_CORALSWARM]
//!     [--source-types github_code,github_code_summary]
//! ```
//!
//! Extraction once read worked examples in test fixtures and code comments as
//! facts, minting `parent_of` / `child_of` / … edges between people in the org
//! ocean. The quality eval counts them (`kinship_edges_from_code`).
//!
//! **Selection.** A `confirmed` edge with a kinship predicate is selected only
//! when EVERY evidence atom it names, whether legacy `provenance.atom_ids` or
//! v2 `provenance.semantic_atom_ids`, resolves and comes from one of
//! `--source-types`. An edge with any evidence from another source, or with
//! no resolvable evidence at all, is never selected.
//!
//! **Execution.** Each selected edge goes through
//! `Store::set_relationship_edge_status(.., Rejected, None, "ops_retract_fixture_kinship")`,
//! the same write a human reject makes in the review UI. It stamps
//! `reviewed_at`/`via`, appends the sync op, and rejects the materialized
//! inverse edge too. Nothing is deleted, and an operator can flip an edge back
//! to confirmed through the same review route.
//!
//! **Dry run by default.** Without `--execute`, the bin only selects, inside
//! `BEGIN READ ONLY` with `SET LOCAL statement_timeout`. It is idempotent:
//! status is re-read before every write, and an edge that is already
//! rejected, for example as the inverse of an earlier one, is skipped.
//!
//! The DSN is read from the environment variable named by `--tenant-dsn-env`
//! (default `TENANT_DB_CORALSWARM`, set on the backend machines), never from
//! argv. A preflight refuses a connection that does not bypass RLS on
//! `relationship_edge`, because a filtered read would undercount silently.
//!
//! Exit codes: 0 done (or nothing to do); 1 a query or write failed; 2 usage;
//! 3 the connection does not bypass RLS.

use std::process::ExitCode;
use std::sync::Arc;

use coralswarm_backend::relationships::EdgeStatus;
use coralswarm_backend::store::{PostgresStore, Store};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use uuid::Uuid;

const KINSHIP: &[&str] = &[
    "parent_of", "child_of", "sibling_of", "spouse_of", "cousin_of", "aunt_uncle_of",
    "niece_nephew_of", "grandparent_of", "grandchild_of", "sibling_in_law_of", "parent_in_law_of",
];
const DEFAULT_TYPES: &[&str] = &["github_code", "github_code_summary"];
const VIA: &str = "ops_retract_fixture_kinship";

/// $1 kinship predicates, $2 allowed evidence source types.
pub(crate) const SELECT_SQL: &str = r#"
WITH ev AS (
  SELECT e.edge_id, a.source_type
    FROM relationship_edge e
    CROSS JOIN LATERAL jsonb_array_elements_text(
      CASE WHEN jsonb_typeof(e.provenance->'atom_ids')='array' THEN e.provenance->'atom_ids' ELSE '[]'::jsonb END) x(id)
    JOIN atoms a ON a.atom_id::text = x.id
   WHERE e.status='confirmed' AND e.predicate = ANY($1)
  UNION ALL
  SELECT e.edge_id, v.source_type
    FROM relationship_edge e
    CROSS JOIN LATERAL jsonb_array_elements_text(
      CASE WHEN jsonb_typeof(e.provenance->'semantic_atom_ids')='array' THEN e.provenance->'semantic_atom_ids' ELSE '[]'::jsonb END) x(id)
    JOIN semantic_atoms s ON s.semantic_atom_id::text = x.id
    JOIN source_versions v ON v.version_id = s.source_version_id
   WHERE e.status='confirmed' AND e.predicate = ANY($1)
)
SELECT e.edge_id, e.graph_id, e.predicate,
       (SELECT array_agg(DISTINCT ev.source_type ORDER BY ev.source_type) FROM ev WHERE ev.edge_id=e.edge_id) evidence_types
  FROM relationship_edge e
 WHERE e.edge_id IN (SELECT edge_id FROM ev GROUP BY edge_id HAVING bool_and(source_type = ANY($2)))
 ORDER BY e.graph_id, e.predicate, e.edge_id
"#;

type Candidate = (Uuid, Uuid, String, Vec<String>);

struct Args {
    execute: bool,
    dsn_env: String,
    types: Vec<String>,
}

fn usage(msg: &str) -> ExitCode {
    eprintln!("retract_fixture_kinship: {msg}");
    eprintln!("usage: retract_fixture_kinship [--execute] [--tenant-dsn-env NAME] [--source-types a,b]");
    ExitCode::from(2)
}

fn parse() -> Result<Args, String> {
    let mut args = Args { execute: false, dsn_env: "TENANT_DB_CORALSWARM".into(), types: DEFAULT_TYPES.iter().map(|s| s.to_string()).collect() };
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--execute" => args.execute = true,
            "--tenant-dsn-env" => args.dsn_env = it.next().ok_or("--tenant-dsn-env needs a value")?,
            "--source-types" => {
                args.types = it.next().ok_or("--source-types needs a value")?.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
                if args.types.is_empty() {
                    return Err("--source-types is empty".into());
                }
            }
            other => return Err(format!("unknown argument {other}")),
        }
    }
    Ok(args)
}

const BYPASS_SQL: &str = "SELECT r.rolbypassrls OR NOT c.relrowsecurity OR (pg_has_role(current_user, c.relowner, 'USAGE') AND NOT c.relforcerowsecurity)
   FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace, pg_roles r
  WHERE n.nspname='public' AND c.relname='relationship_edge' AND r.rolname=current_user";

/// One read-only transaction: RLS-bypass preflight, tenant fingerprint, selection.
pub(crate) async fn inspect(pool: &PgPool, types: &[String]) -> Result<(bool, Option<String>, Vec<Candidate>), sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION READ ONLY").execute(&mut *tx).await?;
    sqlx::query("SET LOCAL statement_timeout='30s'").execute(&mut *tx).await?;
    let bypass: bool = sqlx::query_scalar(BYPASS_SQL).fetch_one(&mut *tx).await?;
    let org: Option<String> = sqlx::query_scalar("SELECT org_id FROM tenant_meta WHERE id = true").fetch_optional(&mut *tx).await?;
    let kin: Vec<String> = KINSHIP.iter().map(|s| s.to_string()).collect();
    let rows: Vec<Candidate> = sqlx::query_as(SELECT_SQL).bind(&kin).bind(types).fetch_all(&mut *tx).await?;
    tx.commit().await?;
    Ok((bypass, org, rows))
}

/// Rejects every still-confirmed candidate; returns (rejected, skipped).
pub(crate) async fn retract(pool: &PgPool, store: &dyn Store, candidates: &[Candidate]) -> Result<(usize, usize), String> {
    let (mut rejected, mut skipped) = (0, 0);
    for (edge, _, predicate, _) in candidates {
        let status: Option<String> = sqlx::query_scalar("SELECT status FROM relationship_edge WHERE edge_id=$1")
            .bind(edge)
            .fetch_optional(pool)
            .await
            .map_err(|e| format!("status read {edge}: {e}"))?;
        if status.as_deref() != Some("confirmed") {
            skipped += 1;
            continue;
        }
        store
            .set_relationship_edge_status(*edge, EdgeStatus::Rejected, None, VIA)
            .await
            .map_err(|e| format!("reject {edge} ({predicate}): {e}"))?;
        rejected += 1;
    }
    Ok((rejected, skipped))
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = match parse() {
        Ok(a) => a,
        Err(e) => return usage(&e),
    };
    let Ok(dsn) = std::env::var(&args.dsn_env) else {
        return usage(&format!("environment variable {} is not set", args.dsn_env));
    };
    let pool = match PgPoolOptions::new().max_connections(2).connect(&dsn).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("connect: {e}");
            return ExitCode::from(1);
        }
    };
    let (bypass, org, candidates) = match inspect(&pool, &args.types).await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("inspect: {e}");
            return ExitCode::from(1);
        }
    };
    if !bypass {
        eprintln!("refusing: this connection does not bypass RLS on relationship_edge, so the selection could undercount");
        return ExitCode::from(3);
    }
    println!("tenant {} | evidence types {:?} | {} candidate edge(s){}", org.as_deref().unwrap_or("?"), args.types, candidates.len(),
        if args.execute { "" } else { " | DRY RUN (pass --execute to reject)" });
    for (edge, graph, predicate, types) in &candidates {
        println!("  {edge}  graph {graph}  {predicate:<18} evidence {}", types.join(","));
    }
    if !args.execute || candidates.is_empty() {
        return ExitCode::SUCCESS;
    }
    let store: Arc<dyn Store> = Arc::new(PostgresStore::new(pool.clone()));
    match retract(&pool, store.as_ref(), &candidates).await {
        Ok((rejected, skipped)) => {
            println!("rejected {rejected}, skipped {skipped} (already not confirmed, e.g. an inverse of an earlier edge)");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("{e}");
            ExitCode::from(1)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::Executor;

    /// Scratch tenant DB on `RELINFER_TEST_DATABASE_URL` (a server URL whose
    /// role may create databases). Skips without it unless REQUIRE_PG_SUITE.
    async fn scratch() -> Option<(coralswarm_backend::test_db::TestDatabase, PgPool)> {
        let Ok(base) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
            assert!(std::env::var_os("REQUIRE_PG_SUITE").is_none(), "REQUIRE_PG_SUITE set but RELINFER_TEST_DATABASE_URL is not");
            return None;
        };
        let name = coralswarm_backend::test_db::TestDatabase::from_url(&base, "retract_kin");
        let admin = PgPool::connect(&base).await.unwrap();
        admin.execute(format!("CREATE DATABASE {name}").as_str()).await.unwrap();
        let url = match base.rfind('/') {
            Some(i) if base[i + 1..].chars().all(|c| c != '@' && c != ':') => format!("{}/{name}", &base[..i]),
            _ => format!("{base}/{name}"),
        };
        let pool = PgPool::connect(&url).await.unwrap();
        sqlx::migrate!("./migrations_tenant").run(&pool).await.unwrap();
        Some((name, pool))
    }

    async fn node(pool: &PgPool, graph: Uuid, title: &str) -> Uuid {
        sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title,body,scope,entity_type) VALUES($1,'entity',$2,'{}'::jsonb,'shared','person') RETURNING node_id")
            .bind(graph).bind(title).fetch_one(pool).await.unwrap()
    }

    async fn atom(pool: &PgPool, graph: Uuid, source_type: &str) -> Uuid {
        sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,scope,timestamp) VALUES($1,$2,$3,'','fixture','shared',now()) RETURNING atom_id")
            .bind(graph).bind(source_type).bind(format!("{source_type}:{}", Uuid::new_v4())).fetch_one(pool).await.unwrap()
    }

    async fn edge(pool: &PgPool, graph: Uuid, src: Uuid, predicate: &str, dst: Uuid, atoms: &[Uuid]) -> Uuid {
        let ids: Vec<String> = atoms.iter().map(Uuid::to_string).collect();
        sqlx::query_scalar("INSERT INTO relationship_edge(graph_id,src_node_id,predicate,dst_node_id,origin,status,confidence,provenance) VALUES($1,$2,$3,$4,'stated','confirmed',0.9,jsonb_build_object('atom_ids',to_jsonb($5::text[]))) RETURNING edge_id")
            .bind(graph).bind(src).bind(predicate).bind(dst).bind(&ids).fetch_one(pool).await.unwrap()
    }

    async fn status(pool: &PgPool, edge: Uuid) -> String {
        sqlx::query_scalar("SELECT status FROM relationship_edge WHERE edge_id=$1").bind(edge).fetch_one(pool).await.unwrap()
    }

    /// Selects only kinship edges whose every evidence atom is code; rejects
    /// them and their inverse through the store; a rerun is a no-op.
    #[tokio::test]
    async fn retracts_only_code_evidenced_kinship_and_is_idempotent_pg() {
        let Some((_db, pool)) = scratch().await else { return };
        let account = Uuid::new_v4();
        let graph: Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind) VALUES($1,'Organization','org') RETURNING id")
            .bind(account).fetch_one(&pool).await.unwrap();
        let (a, b, c) = (node(&pool, graph, "A").await, node(&pool, graph, "B").await, node(&pool, graph, "C").await);
        let code = atom(&pool, graph, "github_code").await;
        let summary = atom(&pool, graph, "github_code_summary").await;
        let slack = atom(&pool, graph, "slack").await;
        let pr = atom(&pool, graph, "github_pr").await;
        let fixture = edge(&pool, graph, a, "parent_of", b, &[code]).await;
        let fixture_inverse = edge(&pool, graph, b, "child_of", a, &[code, summary]).await;
        let mixed = edge(&pool, graph, a, "sibling_of", c, &[code, slack]).await; // real evidence too: keep
        let from_pr = edge(&pool, graph, b, "cousin_of", c, &[pr]).await; // not in the default types: keep
        let not_kin = edge(&pool, graph, a, "works_on", c, &[code]).await; // not kinship: keep
        let dangling = edge(&pool, graph, c, "spouse_of", a, &[Uuid::new_v4()]).await; // unresolvable: keep

        let types: Vec<String> = DEFAULT_TYPES.iter().map(|s| s.to_string()).collect();
        let (bypass, _, found) = inspect(&pool, &types).await.unwrap();
        assert!(bypass, "the scratch owner bypasses RLS");
        let mut ids: Vec<Uuid> = found.iter().map(|c| c.0).collect();
        ids.sort();
        let mut want = vec![fixture, fixture_inverse];
        want.sort();
        assert_eq!(ids, want);

        let store = PostgresStore::new(pool.clone());
        let (rejected, skipped) = retract(&pool, &store, &found).await.unwrap();
        assert_eq!(rejected + skipped, 2);
        assert_eq!(status(&pool, fixture).await, "rejected");
        assert_eq!(status(&pool, fixture_inverse).await, "rejected");
        for keep in [mixed, from_pr, not_kin, dangling] {
            assert_eq!(status(&pool, keep).await, "confirmed");
        }
        let (_, _, again) = inspect(&pool, &types).await.unwrap();
        assert!(again.is_empty(), "a rerun selects nothing");

        let with_pr = vec!["github_code".to_string(), "github_pr".to_string()];
        let (_, _, wider) = inspect(&pool, &with_pr).await.unwrap();
        assert_eq!(wider.iter().map(|c| c.0).collect::<Vec<_>>(), vec![from_pr]);
    }
}
