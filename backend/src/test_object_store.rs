//! A local S3 stand-in for tests that need the object store to fail: the
//! first `accept` object PUTs succeed and every later one answers 503
//! `SlowDown`, as R2 does during an outage. Every other request (GET, HEAD,
//! DELETE) succeeds. It listens on 127.0.0.1, so no test that uses it reaches
//! the network. Test builds only.
//!
//! The counts are distinct objects, not requests: the S3 client retries a
//! `SlowDown` on the same key, and a test asks which objects were written.

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use axum::http::{Method, StatusCode, Uri};
use coralswarm_core::r2::R2Client;

/// The objects the stand-in saw.
#[derive(Clone, Default)]
pub struct Counts {
    attempts: Arc<AtomicUsize>,
    put: Arc<Mutex<HashSet<String>>>,
    deleted: Arc<Mutex<HashSet<String>>>,
}

impl Counts {
    /// Distinct objects a PUT was attempted for, accepted or refused.
    pub fn puts(&self) -> usize {
        self.put.lock().unwrap().len()
    }

    /// Distinct objects deleted (a rollback's cleanup).
    pub fn deletes(&self) -> usize {
        self.deleted.lock().unwrap().len()
    }
}

/// An [`R2Client`] pointed at a fresh stand-in that accepts the first
/// `accept` PUTs, and the stand-in's counts.
pub async fn failing_after(accept: usize) -> (R2Client, Counts) {
    let counts = Counts::default();
    let seen = counts.clone();
    let s3 = axum::Router::new().fallback(move |method: Method, uri: Uri| {
        let seen = seen.clone();
        async move {
            let ok = (StatusCode::OK, [("etag", "\"e2e\"")], String::new());
            if method == Method::DELETE {
                seen.deleted.lock().unwrap().insert(uri.path().to_owned());
                return (StatusCode::NO_CONTENT, [("etag", "")], String::new());
            }
            if method != Method::PUT {
                return ok;
            }
            seen.put.lock().unwrap().insert(uri.path().to_owned());
            if seen.attempts.fetch_add(1, Ordering::SeqCst) < accept {
                ok
            } else {
                (StatusCode::SERVICE_UNAVAILABLE, [("etag", "")], "<Error><Code>SlowDown</Code></Error>".to_string())
            }
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind the S3 stand-in");
    let endpoint = format!("http://{}", listener.local_addr().expect("stand-in address"));
    tokio::spawn(async move {
        let _ = axum::serve(listener, s3).await;
    });
    let r2 = R2Client::new_with_endpoint_override("unused", "key", "secret", "bucket", &endpoint).await;
    (r2, counts)
}
