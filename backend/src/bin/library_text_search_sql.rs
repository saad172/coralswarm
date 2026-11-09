//! Print the actual Library SQL for isolated, repeatable EXPLAIN/equality
//! probes. This test-support binary never connects to a database.
use coralswarm_backend::library_read::{search_sql_for_text_explain, LibraryScope};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        return Err("usage: library_text_search_sql SCOPE_JSON QUERY [--baseline]".into());
    }
    let mut scope: LibraryScope = serde_json::from_str(&args[1])?;
    scope.normalize()?;
    let mut sql = search_sql_for_text_explain(&scope, &args[2]);
    if args.get(3).map(String::as_str) == Some("--baseline") {
        // Freeze the pre-PR matching predicates, not a second evolving copy
        // of admission/version/projection logic. Everything else is identical.
        let start = sql.find("\n, text_matched_outcomes AS").or_else(|| sql.find("\n, matched_atoms AS")).ok_or("matching CTE missing")?;
        let end = sql.find("), title_matched_nodes AS").ok_or("title CTE missing")? + 1;
        sql.replace_range(start..end, include_str!("../../tests/fixtures/library_text_matches_before.sql"));
    }
    println!("{sql}");
    Ok(())
}
