//! `seed_large_ocean` — the W7 large-ocean fixture
//! (`docs/plans/request-reliability-and-scale.md` §6.7).
//!
//! # Why this exists
//!
//! Several workstreams state their acceptance criteria against an ocean far
//! larger than any that exists: W3's request budget at 250k atoms, W5's
//! first-paint target, W7's load/soak scenario. Without a way to *make* such
//! an ocean, every one of those is unverifiable — and the plan already shows
//! what unverifiable numbers cost. Its original W3 gate ("≤ 25 requests")
//! was arithmetically impossible, and nobody noticed because nobody could
//! measure it.
//!
//! So: **build this first in Phase 2.** Several things depend on it, and it
//! is the piece most likely to be underestimated.
//!
//! # What this fixture is NOT, stated up front
//!
//! **Embeddings are `NULL` unless `--embed` is passed, and `--embed` is not
//! implemented here.** Generating 250,000 real 384-dimension embeddings is
//! not a `COPY` — it is the embedding pipeline running for hours, and doing
//! it inline would make seeding unusable in CI. The consequence is precise
//! and must not be forgotten:
//!
//! * Screens that page, filter, sort, or fan out — Reefs, Atlas, the reef
//!   detail view — are **faithfully** represented. Those are exactly the
//!   screens W3/W5/W7 target, and their cost has nothing to do with vector
//!   search.
//! * Anything that goes through retrieval — ask, search, candidates,
//!   relationship inference — is **not**. A vector query against 250,000
//!   `NULL` embeddings returns nothing and returns it instantly, which would
//!   read as "retrieval is fast at scale". It is not a measurement; it is an
//!   empty table.
//!
//! Do not benchmark retrieval against this fixture. If retrieval at scale
//! needs measuring, that is a separate fixture with a real (and expensive)
//! embedding pass, and it should be built deliberately rather than assumed
//! to be this one.
//!
//! # Usage
//!
//! ```bash
//! DATABASE_URL=postgres://… cargo run --bin seed_large_ocean -- \
//!   --atoms 250000 --reefs 100000 --edges-per-reef 7
//! ```
//!
//! Prints the created `graph_id`. Idempotent only in the sense that it always
//! creates a NEW graph — it never mutates an existing one, so it cannot
//! damage a real ocean by being pointed at the wrong database.

use std::time::Instant;

use coralswarm_backend::tenancy::scope::begin_account_scoped;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

/// Rows per bulk insert. Large enough that round-trip cost is amortized,
/// small enough that a single statement's parameter array stays well inside
/// Postgres' limits and memory stays flat.
const BATCH: usize = 5_000;

struct Args {
    atoms: usize,
    reefs: usize,
    edges_per_reef: usize,
}

fn parse_args() -> Args {
    let mut atoms = 250_000usize;
    let mut reefs = 100_000usize;
    let mut edges_per_reef = 7usize;
    let argv: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < argv.len() {
        let next = |i: usize| -> usize {
            argv.get(i + 1)
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(|| panic!("missing or invalid value for {}", argv[i]))
        };
        match argv[i].as_str() {
            "--atoms" => {
                atoms = next(i);
                i += 2;
            }
            "--reefs" => {
                reefs = next(i);
                i += 2;
            }
            "--edges-per-reef" => {
                edges_per_reef = next(i);
                i += 2;
            }
            other => panic!("unknown argument: {other}"),
        }
    }
    Args {
        atoms,
        reefs,
        edges_per_reef,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args();
    let dsn = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    // A small pool: this is one sequential writer, and a big pool against a
    // seeding target just makes it easier to saturate something by accident.
    let pool = PgPoolOptions::new().max_connections(2).connect(&dsn).await?;

    let started = Instant::now();

    // ── Owner scaffolding ────────────────────────────────────────────────────
    // A dedicated account/user per seed run. Sharing a real account would let
    // a fixture ocean show up in someone's UI, and — worse — would make a
    // mistaken `--atoms 250000` against production indistinguishable from
    // real data.
    let account_id: Uuid = sqlx::query_scalar(
        "INSERT INTO accounts (name) VALUES ($1) RETURNING id",
    )
    .bind(format!("w7-fixture-{}", Uuid::new_v4()))
    .fetch_one(&pool)
    .await?;

    // `graphs` IS on 0083's RLS roster, so this INSERT must be scoped like
    // every other one below. This is not a hypothetical: the CORRECTED entry
    // for clerk_provision.rs in scripts/rls_coverage.manifest records that an
    // unscoped `graphs` INSERT is exactly what reached production and made
    // every brand-new user's first authenticated request fail. The coverage
    // gate cannot catch it — it only proves `begin_account_scoped` appears
    // SOMEWHERE in the file — so the same shape nearly shipped again here.
    //
    // Scoping to a just-created account is well-defined: 0083's `graphs`
    // policy compares the new row's own `account_id` to the GUC and never
    // joins `accounts`/`account_members`, so there is no ordering dependency.
    let mut gtx = begin_account_scoped(&pool, account_id).await?;
    let graph_id: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name) VALUES ($1, $2) RETURNING id",
    )
    .bind(account_id)
    .bind("W7 large-ocean fixture")
    .fetch_one(&mut *gtx)
    .await?;
    gtx.commit().await?;

    eprintln!("seeding graph {graph_id} (account {account_id})");

    // ── RLS ──────────────────────────────────────────────────────────────────
    //
    // `atoms`, `nodes` and `node_atoms` are RLS-covered consumer tables whose
    // policies filter on the STRICT `current_setting('app.account_id')`
    // (migrations_control/0083_consumer_rls.sql). "Strict" means a session
    // with the GUC unset does not quietly see nothing — it ERRORS. So every
    // write below runs inside `tenancy::scope::begin_account_scoped`, the one
    // blessed place that issues `SET LOCAL app.account_id`.
    //
    // This is not ceremony for a throwaway tool: a seeder that bypassed RLS
    // would be a seeder that cannot detect an RLS regression, and it would
    // need a privileged role that nothing else in this repo asks for.
    //
    // ── Atoms ────────────────────────────────────────────────────────────────
    // `UNNEST` bulk insert rather than row-by-row: at 250k rows the
    // difference is minutes versus hours, and this is meant to be runnable.
    let mut atom_ids: Vec<Uuid> = Vec::with_capacity(args.atoms);
    let mut done = 0usize;
    while done < args.atoms {
        let n = BATCH.min(args.atoms - done);
        let mut tx = begin_account_scoped(&pool, account_id).await?;
        let ids: Vec<Uuid> = (0..n).map(|_| Uuid::new_v4()).collect();
        let source_ids: Vec<String> = (0..n).map(|i| format!("fixture-{}", done + i)).collect();
        let blob_keys: Vec<String> = (0..n).map(|i| format!("fixture/{}", done + i)).collect();
        // Spread timestamps across a year so date-range and activity-bar
        // queries have a realistic distribution rather than 250k rows landing
        // in one second — a fixture whose timestamps are all identical would
        // make every windowed query trivially fast and prove nothing.
        let ts: Vec<chrono::DateTime<chrono::Utc>> = (0..n)
            .map(|i| chrono::Utc::now() - chrono::Duration::minutes(((done + i) % 525_600) as i64))
            .collect();

        sqlx::query(
            "INSERT INTO atoms (atom_id, graph_id, source_type, source_id, content_blob_key, timestamp) \
             SELECT * FROM UNNEST($1::uuid[], $2::uuid[], $3::text[], $4::text[], $5::text[], $6::timestamptz[])",
        )
        .bind(&ids)
        .bind(&vec![graph_id; n])
        .bind(&vec!["note".to_string(); n])
        .bind(&source_ids)
        .bind(&blob_keys)
        .bind(&ts)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;

        atom_ids.extend_from_slice(&ids);
        done += n;
        if done % 50_000 == 0 || done == args.atoms {
            eprintln!("  atoms: {done}/{}", args.atoms);
        }
    }

    // ── Reefs ────────────────────────────────────────────────────────────────
    let mut reef_ids: Vec<Uuid> = Vec::with_capacity(args.reefs);
    let mut done = 0usize;
    while done < args.reefs {
        let n = BATCH.min(args.reefs - done);
        let mut tx = begin_account_scoped(&pool, account_id).await?;
        let ids: Vec<Uuid> = (0..n).map(|_| Uuid::new_v4()).collect();
        let titles: Vec<String> = (0..n).map(|i| format!("Fixture reef {}", done + i)).collect();
        // All `entity`: the `decisions_require_decided_at` CHECK would need a
        // populated body, and reef TYPE is not a variable any of the
        // workstreams this fixture serves are sensitive to.
        sqlx::query(
            "INSERT INTO nodes (node_id, graph_id, type, title) \
             SELECT * FROM UNNEST($1::uuid[], $2::uuid[], $3::text[], $4::text[])",
        )
        .bind(&ids)
        .bind(&vec![graph_id; n])
        .bind(&vec!["entity".to_string(); n])
        .bind(&titles)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;

        reef_ids.extend_from_slice(&ids);
        done += n;
        if done % 50_000 == 0 || done == args.reefs {
            eprintln!("  reefs: {done}/{}", args.reefs);
        }
    }

    // ── Edges ────────────────────────────────────────────────────────────────
    // `node_atoms` is what W3's paginated edge contract reads and what the
    // drain budget is measured against, so its cardinality is the number that
    // actually matters here — §7 estimates 600–750k at target scale, and this
    // is where that estimate stops being an extrapolation.
    //
    // Atoms are assigned round-robin so every atom belongs to roughly the
    // same number of reefs. Real oceans are power-law (a few hub reefs hold
    // most edges), which is HARDER on the drain budget than this. Noted
    // rather than modelled: a uniform fixture gives an optimistic floor, and
    // a floor that is honest about being one is more useful than a
    // distribution invented to look realistic.
    let mut edge_total = 0usize;
    let mut pending_nodes: Vec<Uuid> = Vec::with_capacity(BATCH);
    let mut pending_atoms: Vec<Uuid> = Vec::with_capacity(BATCH);
    let mut cursor = 0usize;

    for (ri, reef) in reef_ids.iter().enumerate() {
        for _ in 0..args.edges_per_reef {
            if atom_ids.is_empty() {
                break;
            }
            pending_nodes.push(*reef);
            pending_atoms.push(atom_ids[cursor % atom_ids.len()]);
            cursor += 1;
            if pending_nodes.len() >= BATCH {
                flush_edges(&pool, account_id, &mut pending_nodes, &mut pending_atoms).await?;
                edge_total += BATCH;
            }
        }
        if ri % 20_000 == 0 && ri > 0 {
            eprintln!("  edges: {edge_total} (reef {ri}/{})", args.reefs);
        }
    }
    if !pending_nodes.is_empty() {
        edge_total += pending_nodes.len();
        flush_edges(&pool, account_id, &mut pending_nodes, &mut pending_atoms).await?;
    }

    // ANALYZE so the planner has real statistics. Without it the first
    // queries against the fixture are planned against defaults, and a
    // benchmark that measures a cold planner measures the wrong thing.
    sqlx::query("ANALYZE atoms, nodes, node_atoms")
        .execute(&pool)
        .await?;

    eprintln!(
        "done in {:?}: {} atoms, {} reefs, ~{} edges",
        started.elapsed(),
        args.atoms,
        args.reefs,
        edge_total
    );
    eprintln!("NOTE: embeddings are NULL — do NOT benchmark retrieval against this fixture.");
    // The only thing on stdout, so a caller can capture it directly.
    println!("{graph_id}");
    Ok(())
}

async fn flush_edges(
    pool: &sqlx::PgPool,
    account_id: Uuid,
    nodes: &mut Vec<Uuid>,
    atoms: &mut Vec<Uuid>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut tx = begin_account_scoped(pool, account_id).await?;
    // `ON CONFLICT DO NOTHING`: round-robin assignment can pair the same
    // (reef, atom) twice when `edges_per_reef` exceeds the atom count, and a
    // fixture that crashes on its own arithmetic is worse than one that
    // silently produces slightly fewer edges than requested.
    sqlx::query(
        "INSERT INTO node_atoms (node_id, atom_id) \
         SELECT * FROM UNNEST($1::uuid[], $2::uuid[]) ON CONFLICT DO NOTHING",
    )
    .bind(&*nodes)
    .bind(&*atoms)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    nodes.clear();
    atoms.clear();
    Ok(())
}
