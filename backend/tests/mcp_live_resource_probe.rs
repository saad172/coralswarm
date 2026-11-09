//! Manual live-issuer acceptance probe. All inputs stay in private token files;
//! the test is ignored in CI and never prints token contents or identities.
//! Set CS_LIVE_TOKEN_DIR and CS_LIVE_EXPECT_RESOURCE_BOUND=bound|unbound, then
//! run `cargo test -p coralswarm-backend --test mcp_live_resource_probe --
//! --ignored --nocapture`. The directory contains initial-access-token.txt,
//! refreshed-resource-access-token.txt and refreshed-inherited-access-token.txt.
use coralswarm_backend::clerk::{verify, verify_oauth, ClerkConfig};

#[tokio::test]
#[ignore = "requires private live reviewer token files and explicit bound/unbound expectation"]
async fn live_review_tokens_match_the_expected_resource_binding() {
    let dir = std::env::var("CS_LIVE_TOKEN_DIR").expect("private token directory required");
    let bound = match std::env::var("CS_LIVE_EXPECT_RESOURCE_BOUND").as_deref() {
        Ok("bound") => true,
        Ok("unbound") => false,
        _ => panic!("set CS_LIVE_EXPECT_RESOURCE_BOUND to bound or unbound"),
    };
    let cfg = ClerkConfig {
        issuer: "https://clerk.coralswarm.example".into(),
        jwks_url: "https://clerk.coralswarm.example/.well-known/jwks.json".into(),
    };
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .build()
        .unwrap();
    for name in [
        "initial-access-token.txt",
        "refreshed-resource-access-token.txt",
        "refreshed-inherited-access-token.txt",
    ] {
        let token = std::fs::read_to_string(std::path::Path::new(&dir).join(name)).unwrap();
        // Prove signature/issuer/time validity first; do not print claims.
        assert!(
            verify(token.trim(), &cfg, &http).await.is_ok(),
            "signature/issuer/time validation failed for {name}"
        );
        let result =
            verify_oauth(token.trim(), &cfg, &http, "https://api.coralswarm.example/mcp").await;
        if bound {
            assert!(result.is_ok(), "resource-bound token rejected for {name}");
            println!("{name}: signature/issuer/time/resource valid; strict verifier accepted");
        } else {
            assert!(
                matches!(result, Err(coralswarm_backend::clerk::ClerkError::Invalid(ref reason)) if reason.contains("aud")),
                "expected missing/wrong audience rejection for {name}"
            );
            println!("{name}: signature/issuer/time valid; strict verifier rejected audience");
        }
    }
}
