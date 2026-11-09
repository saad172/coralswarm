//! Live end-to-end test for contextual chunk headers (cs-phase0-chunk-headers)
//! against a REAL Postgres.
//!
//! Proves, against the database:
//!   * `atoms.header_len` (migration 0066) round-trips through `insert_atoms` +
//!     a `header_len` stamp (mirroring `routes/ingest.rs` / `workers/huddle.rs`).
//!   * `Store::atom_content_refs`, `Store::atom_inline_content`, and
//!     `Store::atom_detail` all report the stamped `header_len`.
//!   * `store::strip_header_prefix` applied to what those three methods return
//!     recovers exactly the original (header-free) chunk text — the citation-
//!     cleanliness contract this feature exists for.
//!   * An atom with NO header (`header_len` left NULL/0) round-trips its content
//!     unchanged (`strip_header_prefix` is a no-op).
//!
//! Gated on `RELINFER_TEST_DATABASE_URL` (a Postgres with all migrations
//! applied); a normal `cargo test` with no DB configured skips it.
//!
//! Run with:
//!   RELINFER_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55432/coralswarm \
//!     cargo test -p coralswarm-backend --test header_len_pg -- --nocapture

use chrono::Utc;
use coralswarm_backend::store::{strip_header_prefix, PostgresStore, Store};
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

#[tokio::test]
async fn header_len_round_trips_and_strips_cleanly_against_real_postgres() {
    let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
        Ok(u) => u,
        Err(_) => {
            eprintln!("skipping: set RELINFER_TEST_DATABASE_URL to run the live header_len e2e");
            return;
        }
    };

    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .expect("connect to test Postgres");

    // ── Seed tenancy chain: user → account → graph ────────────────────────────
    let email = format!("{}@test.local", Uuid::new_v4());
    let user_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(&email)
        .fetch_one(&pool)
        .await
        .expect("insert user");
    let account_id: Uuid =
        sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
            .bind(user_id)
            .fetch_one(&pool)
            .await
            .expect("insert account");

    let store = PostgresStore::new(pool.clone());
    let graph = store
        .create_graph(account_id, "header-len-e2e", "personal")
        .await
        .expect("create graph");
    let g = graph.id;

    // ── Atom WITH a contextual chunk header ────────────────────────────────────
    let header = "Meeting: Weekly Standup, 2026-07-15. Participants: José, 東京.";
    let clean_body = "Alice: let's ship the header feature this week.";
    let full_text = format!("{header}\n\n{clean_body}");
    let header_len = (header.len() + 2) as i32;

    let header_atom_id = Uuid::new_v4();
    let source_id = format!("meeting-{}:chunk:0", Uuid::new_v4());
    let inserted = store
        .insert_atoms(
            &[header_atom_id],
            g,
            "transcript",
            &[source_id],
            &[format!("{g}/{header_atom_id}")],
            &["Alice".to_string(), "Bob".to_string()],
            Utc::now(),
            user_id,
            "private",
            "english",
        )
        .await
        .expect("insert header atom");
    assert_eq!(inserted, vec![header_atom_id]);

    store
        .set_atoms_inline_content(&[header_atom_id], &[full_text.clone()])
        .await
        .expect("set inline content");

    // Stamp header_len exactly as ingest::submit (step
    // 8b-2) and workers/huddle.rs::ingest_huddle_transcript do.
    sqlx::query("UPDATE atoms SET header_len = $2 WHERE atom_id = $1")
        .bind(header_atom_id)
        .bind(header_len)
        .execute(&pool)
        .await
        .expect("stamp header_len");

    // ── Atom WITHOUT a header (e.g. a Slack message) ───────────────────────────
    let plain_atom_id = Uuid::new_v4();
    let plain_text = "just a plain Slack message, no contextual header".to_string();
    let plain_source_id = format!("slack-{}:chunk:0", Uuid::new_v4());
    store
        .insert_atoms(
            &[plain_atom_id],
            g,
            "slack",
            &[plain_source_id],
            &[format!("{g}/{plain_atom_id}")],
            &[],
            Utc::now(),
            user_id,
            "private",
            "english",
        )
        .await
        .expect("insert plain atom");
    store
        .set_atoms_inline_content(&[plain_atom_id], &[plain_text.clone()])
        .await
        .expect("set inline content (plain)");
    // header_len deliberately left NULL — no UPDATE.

    // ── atom_content_refs reports header_len for both ──────────────────────────
    let refs = store
        .atom_content_refs(&[header_atom_id, plain_atom_id])
        .await
        .expect("atom_content_refs");
    let refs_by_id: std::collections::HashMap<Uuid, i32> =
        refs.iter().map(|(id, _, _, _, hl)| (*id, *hl)).collect();
    assert_eq!(refs_by_id[&header_atom_id], header_len);
    assert_eq!(refs_by_id[&plain_atom_id], 0, "NULL header_len reads as 0");

    // ── atom_inline_content reports header_len AND the content strips clean ────
    let inline = store
        .atom_inline_content(&[header_atom_id, plain_atom_id])
        .await
        .expect("atom_inline_content");
    assert_eq!(inline.len(), 2);
    assert!(inline.iter().any(|(id, ..)| *id == header_atom_id));
    assert!(inline.iter().any(|(id, ..)| *id == plain_atom_id));
    for (id, content, _source_type, _ts, hl) in &inline {
        let content = content.as_deref().expect("content_inline set");
        let clean = strip_header_prefix(content, *hl);
        if *id == header_atom_id {
            assert_eq!(*hl, header_len);
            assert_eq!(
                clean, clean_body,
                "header must be stripped, leaving exactly the original chunk text"
            );
            assert!(
                !clean.contains("Meeting:"),
                "stripped content must not leak the contextual header"
            );
        } else {
            assert_eq!(*hl, 0);
            assert_eq!(clean, plain_text, "no-header atom must round-trip unchanged");
        }
    }

    // ── atom_detail (the drill-through / citation detail view) agrees ──────────
    let detail = store
        .atom_detail(header_atom_id, account_id)
        .await
        .expect("atom_detail query")
        .expect("atom_detail found");
    assert_eq!(detail.header_len, header_len);
    let detail_content = detail.content_inline.expect("content_inline set");
    let detail_clean = strip_header_prefix(&detail_content, detail.header_len);
    assert_eq!(detail_clean, clean_body);
    assert!(!detail_clean.contains("Meeting:"));

    let plain_detail = store
        .atom_detail(plain_atom_id, account_id)
        .await
        .expect("atom_detail query (plain)")
        .expect("atom_detail found (plain)");
    assert_eq!(plain_detail.header_len, 0);
}
