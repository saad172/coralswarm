//! Thin binary entry point.
//!
//! The engine and HTTP server live in the `coralswarm_backend` *library* crate
//! (`src/lib.rs`). This binary just boots the cloud server via [`coralswarm_backend::run`].

fn main() {
    match std::env::args().skip(1).collect::<Vec<_>>().as_slice() {
        [] => {},
        [command] if command == "--financial-release-capability" => {
            println!("{{\"financial_release\":\"account-entry-c1\",\"managed_recovery\":\"legacy-and-v1\",\"admission_disable\":\"MANAGED_PROCESSING_DISABLED\"}}");
            return;
        },
        _ => { eprintln!("unsupported backend argument"); std::process::exit(2); },
    }
    // `run()` returns `Err` (instead of panicking) for a startup failure that
    // should exit the process cleanly and non-zero — e.g. a release build
    // with no usable ONNX embedder. The same fail-fast idiom the now-removed
    // `mcp-server` stdio crate used to follow for its own `ServerConfig::from_env()`.
    let runtime=tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("Tokio runtime");
    if let Err(e) = runtime.block_on(coralswarm_backend::run()) {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}
