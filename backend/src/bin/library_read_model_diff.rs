//! Prints the compiled Library reader's descriptor (`--describe`) or its twelve
//! sealed `reader_program` records (`--export-sql`). No database, no
//! credentials. The release build's "Validate generated Library verdicts" step
//! feeds the export to `scripts/check_verdict_once.py`.
use coralswarm_backend::library_read::{read_model, LibraryScope};
use serde_json::json;
use uuid::Uuid;

fn main() {
    if let Err(category) = run() {
        eprintln!("{}", json!({"complete": false, "error_category": category}));
        std::process::exit(1);
    }
}

fn descriptor() -> serde_json::Value {
    json!({"source_sha": env!("CORALSWARM_GIT_SHA_FULL"), "model_sql_sha256": read_model::definition_sha256(),
        "definition_version": read_model::DEFINITION_VERSION, "context_version": read_model::CONTEXT_VERSION, "program_version": read_model::PROGRAM_VERSION})
}

fn run() -> Result<(), &'static str> {
    if std::env::args().any(|arg| arg == "--describe") {
        println!("{}", descriptor());
        return Ok(());
    }
    if std::env::args().any(|arg| arg == "--export-sql") {
        // Every variant retains the actual sixteen request binds and both
        // stages of the versioned RR program.
        for mode in ["all_retained", "current"] { for filtered in [false, true] { for (text_strategy, text_query) in read_model::DEFINITION_QUERY_VARIANTS {
            let mut shape = json!({"graph_ids": [Uuid::nil()], "outcome_versions": mode});
            if filtered { shape["collection"] = json!("meetings"); }
            let mut scope: LibraryScope = serde_json::from_value(shape).map_err(|_| "scope_decode")?;
            scope.normalize().map_err(|_| "scope_normalize")?;
            let mut record = descriptor();
            for (key, value) in [("kind", json!("reader_program")), ("outcome_versions", json!(mode)), ("filtered", json!(filtered)),
                ("text_strategy", json!(text_strategy)), ("program", read_model::definition_program(&scope, text_query)),
                ("bind_types", json!(["uuid[]", "jsonb", "uuid", "uuid", "uuid", "text", "text", "timestamptz", "uuid", "timestamptz", "uuid", "bigint", "bigint", "uuid", "bigint", "timestamptz"])),
                ("query_settings", json!({"statement_timeout": "12s", "plan_cache_mode": "force_custom_plan", "jit": "off", "work_mem": "64MB", "max_parallel_workers_per_gather": "0"})),
                ("page_limit_bind", json!("requested_page_size_plus_one"))] {
                record[key] = value;
            }
            println!("{record}");
        }}}
        return Ok(());
    }
    Err("usage: library_read_model_diff --describe | --export-sql")
}
