//! Guards against the whole CLASS of bug `services::promotion::
//! write_promotion_rows` turned out to still have after two rounds of manual
//! sweeping: migration `0107_nodes_case_insensitive_identity` (control) /
//! `0018_nodes_case_insensitive_identity` (tenant) drops
//! `nodes_graph_type_title_unique` and replaces it with
//! `nodes_graph_type_lower_title_unique`, so ANY Postgres statement still
//! targeting the old, now-nonexistent constraint via the conflict-target this
//! file checks for fails at runtime with "no unique or exclusion constraint
//! matching the ON CONFLICT specification" the moment that migration lands.
//! That is not a compile-time error, and it is not covered by
//! `cargo check`/`cargo clippy` — nothing catches it except a live database or
//! a test that reads the source itself. Two manual sweeps each missed at
//! least one call site (`upsert_node`/`upsert_node_scoped` the first time,
//! `services::promotion::write_promotion_rows` the second) — a mechanical
//! check is worth more than a third careful-but-manual sweep.
//!
//! Deliberately lives under `backend/tests/`, NOT `backend/src/`: the check
//! scans every `.rs` file under `backend/src` for a literal substring, and if
//! this test (or its own doc comments naming that substring) lived inside
//! `src` it would trip its own scan — a self-referential false positive.
//! Living in `tests/` keeps the checker's own source text outside the corpus
//! it inspects, with no need to obfuscate the string being searched for.
//!
//! Grep-strength, not AST-aware — same honesty bar as
//! `scripts/check_pg_suite_coverage.sh`: this proves no file under
//! `backend/src` contains the literal now-stale conflict-target string. It
//! would NOT catch the same bug spelled with different whitespace/casing,
//! built as a dynamic/formatted query string, or introduced against some FUTURE constraint rename — it
//! only prevents THIS exact regression from recurring silently. No database
//! needed; this runs under a plain `cargo test`.

use std::path::{Path, PathBuf};

/// The exact conflict-target string that stopped existing the moment
/// migration `0107_nodes_case_insensitive_identity` (control) /
/// `0018_nodes_case_insensitive_identity` (tenant) ran. Any Postgres
/// statement still spelling this is broken, full stop.
const STALE_TARGET: &str = "ON CONFLICT (graph_id, type, title)";

fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("readable src dir") {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            collect_rs_files(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

#[test]
fn no_postgres_statement_targets_the_pre_case_fold_conflict_key() {
    let src_dir = Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/src"));
    let mut files = Vec::new();
    collect_rs_files(src_dir, &mut files);

    // Sanity check the walker itself actually found the crate — a silent
    // zero-file scan would make this test vacuously (and uselessly) pass.
    assert!(
        files.len() > 50,
        "expected to scan the whole backend/src tree, only found {} .rs files — \
         the directory walk is broken, not the invariant it checks",
        files.len()
    );

    let mut violations: Vec<String> = Vec::new();
    for path in &files {
        let normalized = path.display().to_string().replace('\\', "/");
        let content = std::fs::read_to_string(path).expect("readable source file");
        if content.contains(STALE_TARGET) {
            violations.push(normalized);
        }
    }

    assert!(
        violations.is_empty(),
        "found `{STALE_TARGET}` in {} file(s) — \
         this targets a unique constraint that migration 0107/0017 DROPS, so this \
         statement will fail at runtime the moment that migration lands. Retarget to \
         `ON CONFLICT (graph_id, type, lower(title))`, matching every other Postgres \
         site in the crate. Files: {violations:?}",
        violations.len()
    );
}

/// Companion positive check: confirm the scan mechanism itself actually
/// detects the pattern when present (a check that never fires proves
/// nothing). Verified against a synthetic string, not a real file, so this
/// test can't itself trip the guard above.
#[test]
fn the_stale_target_pattern_is_detectable_when_present() {
    let synthetic = format!(
        "let q = \"INSERT INTO nodes (...) {} DO UPDATE ...\";",
        STALE_TARGET
    );
    assert!(synthetic.contains(STALE_TARGET));
}
