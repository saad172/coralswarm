//! Build script: stamps the short git commit SHA into the build so
//! `mcp::server`'s serverInfo can report something more specific than the
//! bare crate version (INFO item, MCP listing-prep #900 correctness review:
//! `CARGO_PKG_VERSION` resolves to `Cargo.toml`'s `0.1.0`, which has never
//! been bumped by `release.sh` or any deploy workflow — every build reports
//! the exact same version string forever, which a directory listing would
//! show unchanged across every release). Appending the commit makes the
//! serverInfo actually distinguish one deployed build from another, which is
//! the whole point of publishing a version at all.
//!
//! Best-effort: `git` may not be on PATH, this may not even be a git
//! checkout (a source tarball, `Dockerfile.prebuilt`'s COPY-only runtime
//! stage never runs this at all since it copies an already-built binary),
//! or the command may simply fail — none of that may ever fail the build
//! itself, so every branch below falls back to a fixed placeholder rather
//! than propagating an `Err`.

use std::{path::Path, process::Command};

fn git_output(args: &[&str]) -> Option<String> {
    Command::new("git")
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn watch_git_head() {
    if let Some(head) = git_output(&["rev-parse", "--git-path", "HEAD"]) {
        if Path::new(&head).is_file() {
            println!("cargo:rerun-if-changed={head}");
        }
    }
    let Some(reference) = git_output(&["symbolic-ref", "-q", "HEAD"]) else {
        return;
    };
    let Some(path) = git_output(&["rev-parse", "--git-path", &reference]) else {
        return;
    };
    let path = Path::new(&path);
    if path.is_file() {
        println!("cargo:rerun-if-changed={}", path.display());
    } else {
        if let Some(packed) = git_output(&["rev-parse", "--git-path", "packed-refs"]) {
            if Path::new(&packed).is_file() {
                println!("cargo:rerun-if-changed={packed}");
            }
        }
        // A packed ref can become loose on the next commit. Watch an existing
        // parent until then; a missing rerun path would make every build dirty.
        if let Some(parent) = path.ancestors().skip(1).find(|p| p.is_dir()) {
            println!("cargo:rerun-if-changed={}", parent.display());
        }
    }
}

fn main() {
    // Second security review, L2: prefer an env var the BUILD ENVIRONMENT
    // itself already knows the true commit from (`GITHUB_SHA` — GitHub
    // Actions' own ambient var, always the full 40-char sha of the commit
    // being built; `SOURCE_COMMIT` — the convention some container-build
    // platforms, e.g. Docker Hub automated builds, set) over shelling out to
    // `git rev-parse` at all. This matters specifically for a Docker build
    // context that COPIES the source tree in WITHOUT its `.git` directory
    // (a common, smaller-image-layer practice) — `git rev-parse` there would
    // either fail outright or, worse, silently resolve to whatever `.git`
    // metadata happens to be present in the build context, which need not
    // be the commit actually being built at all.
    let env_sha = std::env::var("GITHUB_SHA")
        .ok()
        .or_else(|| std::env::var("SOURCE_COMMIT").ok())
        .filter(|s| !s.trim().is_empty())
        .map(|s| s.trim().chars().take(12).collect::<String>());
    if env_sha.is_none() {
        watch_git_head();
    }
    let git_sha = env_sha
        .or_else(|| git_output(&["rev-parse", "--short=12", "HEAD"]))
        .unwrap_or_else(|| "unknown".to_string());

    println!("cargo:rustc-env=CORALSWARM_GIT_SHA={git_sha}");
    let full_sha = std::env::var("GITHUB_SHA").ok()
        .or_else(|| std::env::var("SOURCE_COMMIT").ok())
        .or_else(|| git_output(&["rev-parse", "HEAD"]))
        .filter(|s| s.len()==40 && s.bytes().all(|c| c.is_ascii_hexdigit()))
        .map(|s| s.to_ascii_lowercase()).unwrap_or_else(|| "unknown".into());
    println!("cargo:rustc-env=CORALSWARM_GIT_SHA_FULL={full_sha}");

    // Rerun if either env var's presence/value changes between invocations
    // (e.g. a local `cargo build` after a CI build reused the same target
    // dir) — without this, cargo has no way to know THESE inputs even exist,
    // since they're read here, not passed as a normal `rustc` argument.
    println!("cargo:rerun-if-env-changed=GITHUB_SHA");
    println!("cargo:rerun-if-env-changed=SOURCE_COMMIT");

    println!("cargo:rerun-if-changed=build.rs");

    e2e_build_guard();
}

/// The `e2e` feature (the e2e harness's seams and seeding route, see
/// `src/e2e_seed.rs`) may only ever be compiled into an UNOPTIMIZED build that
/// explicitly declares itself an e2e build. `lib.rs` already refuses it without
/// `debug_assertions`; this also refuses `--release` with debug assertions
/// switched back on, any other optimized profile, and an accidental
/// `--features e2e`/`--all-features` that did not set the marker.
fn e2e_build_guard() {
    println!("cargo:rerun-if-env-changed=CORALSWARM_E2E_BUILD");
    if std::env::var_os("CARGO_FEATURE_E2E").is_none() {
        return;
    }
    let opt = std::env::var("OPT_LEVEL").unwrap_or_default();
    let marked = std::env::var("CORALSWARM_E2E_BUILD").as_deref() == Ok("1");
    if opt != "0" || !marked {
        panic!(
            "the `e2e` feature is for the e2e harness's debug build only: it needs opt-level 0 (got {opt:?}) and CORALSWARM_E2E_BUILD=1 (set: {marked}). It must never be part of a deployable binary."
        );
    }
}
