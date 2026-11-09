//! Query-audit disclosure log (m365 enterprise train PR 4.3, plan decision
//! #11 part 2; migrations `migrations_control/0191_query_audit_log.sql` /
//! `migrations_tenant/0069_query_audit_log.sql`). Spec: "Who asked, oceans
//! searched, atoms cited; org-admin-visible only; disclosed to users. Emit
//! from the ask/retrieval path; admin route + disclosure copy."
//!
//! Sibling spec 4.2 (`connector_audit_log`, the append-only CONNECTOR event
//! log) is a SEPARATE, NOT-yet-built table — this module has nothing to do
//! with it.
//!
//! # Contrast with `mcp::audit` — and why this is NOT that
//!
//! [`crate::mcp::audit::record`] is the existing precedent for a
//! best-effort, log-then-drop audit write: `tokio::spawn`, fully detached,
//! a failure is a `warn!` the request never observes. That shape is correct
//! for a HIGH-FREQUENCY, LOW-STAKES signal (every MCP tool call, success or
//! denial) where losing one row to a transient DB hiccup is an acceptable,
//! invisible cost — the request that produced it has nothing further to say
//! about it.
//!
//! # The failure-mode decision (flagged for founder review)
//!
//! 4.2's own compliance-log durability bar — "audit is the product; write
//! failures fail the action or queue durably, not warn-and-drop" — does NOT
//! automatically transfer to this table, and this module deliberately does
//! not inherit it. `query_audit_log` exists for DISCLOSURE/TRANSPARENCY to
//! an org's admins about what its members asked; it is not itself a
//! compliance control gating anything (nothing downstream reads it to
//! decide whether to permit a future action). Failing a member's ask
//! because a logging insert failed would sacrifice the actual product (an
//! answer the member is legitimately entitled to) to protect a FEATURE
//! whose entire purpose is telling the org what already, successfully,
//! happened. A rare missing disclosure row is a strictly better outcome for
//! the org than an ask that 500s because its own audit trail couldn't be
//! written.
//!
//! **Decision: DURABLE-WARN, not fail-closed.** Two things distinguish this
//! from a plain warn-and-drop (`mcp::audit`'s shape):
//!
//! 1. [`record`] is **awaited inline**, as part of the same request/task
//!    that produced the citations — never `tokio::spawn`ed the way
//!    `mcp::audit::record` / `routes::queries::spawn_query_history` are. The
//!    insert genuinely runs as part of handling the request (or, for the MCP
//!    legacy-task path, the same background completion future that already
//!    owns the work), rather than being handed to a detached task that could
//!    be dropped by a process recycle before it ever executes. This is the
//!    "durable" half: a real, awaited attempt, not a best-effort fire.
//! 2. A failure is logged at `error!`, not `warn!` (an incident-grade
//!    signal, not a warning-grade one): a broken
//!    query-audit write on a healthy DB is a real regression an operator
//!    should page on, even though it must never surface to the asking user.
//!
//! There is no durable retry queue in this PR — a lost row on genuine,
//! sustained DB unavailability is an accepted residual, not a promise this
//! module makes. If the founder wants the harder guarantee (fail the ask
//! closed, or a durable outbox), that is a deliberate escalation to make
//! explicitly, not a default this module assumes.
//!
//! # Digest, not payload — and KEYED, unlike the mcp precedent
//!
//! [`question_digest`] is a keyed HMAC-SHA256 hex digest of the asked
//! question text — the raw text is NEVER stored. The digest-not-payload
//! posture follows `mcp_tool_calls.args_digest` (`mcp::audit`, migration
//! 0058), but deliberately NOT its bare-SHA-256 mechanism (CodeRabbit round
//! 1 on PR 4.3): questions are low-entropy text, so an unkeyed hash lets a
//! log reader confirm candidate questions by offline hashing. Keying on a
//! server-held secret keeps the digest's only real job (server-side
//! correlation) while removing that oracle. An admin reading the disclosure
//! log learns THAT a question was asked and what it touched (which oceans,
//! which atoms), never its content.
//!
//! # The SELECT/RLS split
//!
//! See both migrations' headers for the full reasoning. In short: RLS
//! policies read Postgres session GUCs, which cannot represent a Clerk
//! org-role claim — so "org-admin-only" visibility is enforced at
//! [`crate::routes::query_audit::list_query_audit`], the ROUTE layer, via
//! [`crate::services::deletion::is_admin_role`], BEFORE any row is read.
//! RLS's job here is narrower: the account/tenant boundary only.

#[cfg(test)]
use sha2::{Digest, Sha256};
#[cfg(test)]
use sqlx::PgPool;
use uuid::Uuid;

/// Defensive ceiling on `graphs_searched` mirrored from both migrations'
/// `query_audit_log_graphs_bounded` CHECK constraint. MCP `ask_ocean` accepts
/// at most 5 `ocean_ids` today; this leaves headroom without being
/// unbounded, and truncating in Rust (rather than letting the CHECK
/// constraint reject the whole insert) means an oversized list degrades the
/// audit row instead of losing it entirely.
#[cfg(test)]
const MAX_GRAPHS_SEARCHED: usize = 10;
/// Defensive ceiling on `atom_ids_cited`, mirrored from both migrations'
/// `query_audit_log_atoms_bounded` CHECK constraint. This leaves headroom
/// for citation lists assembled from more than one ocean.
#[cfg(test)]
const MAX_ATOMS_CITED: usize = 200;

/// Disclosure copy (plan decision #11: "disclosed to users"). Surfaced today
/// in [`crate::routes::query_audit::list_query_audit`]'s own response body
/// (every caller of `GET /v1/query-audit` — i.e. every org admin who reads
/// it — sees this line) and in `docs/query-audit-disclosure.md`. A
/// first-class in-product surface reaching every ORDINARY MEMBER (not just
/// admins reading the log itself) — e.g. a settings-page note — is
/// explicitly NOT built in this PR; flagged as follow-up in the PR report.
pub(crate) const DISCLOSURE_TEXT: &str =
    "CoralSwarm logs who asks a question, which oceans were searched, and which sources were \
     cited — never the question's content. This log is visible to your organization's admins.";

/// Keyed HMAC-SHA256 hex digest of a raw query/question string —
/// disclosure without content. NOT a bare hash (CodeRabbit round 1):
/// questions are low-entropy text, so an unsalted SHA-256 would let anyone
/// with read access to `query_audit_log` confirm candidate questions by
/// offline hashing. Keying on a server-held secret removes that oracle —
/// only the server can produce a matching digest, and the digest's only
/// job is server-side correlation ("these two rows asked the same thing"),
/// which HMAC preserves.
///
/// Key material: the [`crate::crypto::kdf::purpose::QUERY_AUDIT_DIGEST`]
/// HKDF derivation of `QUERY_AUDIT_HMAC_KEY` if set, else of
/// `OAUTH_STATE_SECRET` — never either secret raw. The question text is
/// caller-chosen and the digest is returned by `GET /v1/query-audit`, so this
/// function is an HMAC oracle over arbitrary text for its own key; with a raw
/// `OAUTH_STATE_SECRET` key it handed out valid HS256 OAuth `state`
/// signatures (`HMAC(key, "<header>.<payload>")`). The derived key is used
/// for nothing else, so the oracle is worthless outside this column. See
/// `crypto::kdf`'s module doc.
///
/// Rotating the secret (or the derivation) changes future digests, which only
/// costs cross-rotation correlation: nothing compares a stored digest with a
/// freshly computed one (the column is written once and only listed back).
/// With NEITHER secret set (bare dev environments), the digest is prefixed
/// `unkeyed:` so a weaker-than-promised digest is visibly marked in the data
/// rather than silently indistinguishable.
#[cfg(test)]
pub(crate) fn question_digest(question: &str) -> String {
    let non_empty = |name: &str| std::env::var(name).ok().filter(|k| !k.is_empty());
    let secret = non_empty("QUERY_AUDIT_HMAC_KEY").or_else(|| non_empty("OAUTH_STATE_SECRET"));
    digest_for_secret(question, secret.as_deref())
}

/// The pure digest core behind [`question_digest`]: HMAC-SHA256 of
/// `question` under the query-audit derivation of `secret`. Split out so both
/// key states are unit-testable without racing process-global env vars. Both
/// output shapes satisfy the migrations' `^(unkeyed:)?[0-9a-f]{64}$` CHECK.
#[cfg(test)]
pub(crate) fn digest_for_secret(question: &str, secret: Option<&str>) -> String {
    use crate::crypto::kdf::{derive_key, purpose};
    use hmac::{Hmac, Mac};
    match secret {
        Some(s) => {
            let key = derive_key(s.as_bytes(), purpose::QUERY_AUDIT_DIGEST);
            let mut mac = Hmac::<Sha256>::new_from_slice(&key)
                .expect("HMAC-SHA256 accepts any key length");
            mac.update(question.as_bytes());
            hex::encode(mac.finalize().into_bytes())
        }
        None => {
            let mut hasher = Sha256::new();
            hasher.update(question.as_bytes());
            format!("unkeyed:{}", hex::encode(hasher.finalize()))
        }
    }
}

/// Write one `query_audit_log` row. See the module doc's "failure-mode
/// decision" section: this is AWAITED by the caller (never `tokio::spawn`ed)
/// but a write failure is only logged, never propagated to the caller — the
/// ask/search this describes has already succeeded (or honestly abstained)
/// by the time this runs, and that outcome must reach the caller regardless
/// of whether its own disclosure row landed.
///
/// Test-only since the legacy Ask path that called it was removed; the
/// route tests seed `query_audit_log` through it.
///
/// Scopes the INSERT via [`crate::tenancy::scope::begin_tenant_scoped`] —
/// plane-agnostic (sets both `app.account_id` and `app.user_id`), matching
/// both migrations' `WITH CHECK (account_id = app.account_id AND
/// asked_by_user_id = app.user_id-or-tenant_rls_actor_user_id())` INSERT
/// policy.
#[cfg(test)]
pub(crate) async fn record(
    pool: &PgPool,
    account_id: Uuid,
    asked_by_user_id: Uuid,
    question: &str,
    graphs_searched: &[Uuid],
    atom_ids_cited: &[Uuid],
) {
    let digest = question_digest(question);
    let mut graphs: Vec<Uuid> = graphs_searched.to_vec();
    graphs.truncate(MAX_GRAPHS_SEARCHED);
    let mut atoms: Vec<Uuid> = atom_ids_cited.to_vec();
    atoms.truncate(MAX_ATOMS_CITED);

    // The whole scoped write runs under one small time budget (CodeRabbit
    // round 1): this is awaited on the request path AFTER the answer is
    // already computed, so a DB that accepts the connection but stalls on
    // the statement must cost the caller at most this budget — not a
    // TCP-give-up — before the durable-warn path takes over.
    let write = async {
        match crate::tenancy::scope::begin_tenant_scoped(pool, account_id, asked_by_user_id).await {
            Ok(mut tx) => {
                let insert = sqlx::query(
                    "INSERT INTO query_audit_log \
                        (account_id, asked_by_user_id, question_digest, graphs_searched, atom_ids_cited) \
                     VALUES ($1, $2, $3, $4, $5)",
                )
                .bind(account_id)
                .bind(asked_by_user_id)
                .bind(&digest)
                .bind(&graphs)
                .bind(&atoms)
                .execute(&mut *tx)
                .await;
                match insert {
                    Ok(_) => tx.commit().await.map_err(|e| e.to_string()),
                    Err(e) => Err(e.to_string()),
                }
            }
            Err(e) => Err(format!("failed to open scoped transaction: {e}")),
        }
    };
    let outcome = match tokio::time::timeout(std::time::Duration::from_secs(5), write).await {
        Ok(res) => res,
        Err(_) => Err("query_audit_log write exceeded its 5s budget".to_string()),
    };
    if let Err(e) = outcome {
        tracing::error!(
            error = %e,
            account_id = %account_id,
            asked_by_user_id = %asked_by_user_id,
            "query_audit_log insert failed — this ask/search will not appear in the \
             org's disclosure log (durable-warn: the query itself already succeeded \
             for the caller and is unaffected)"
        );
    }
}

/// One page of `query_audit_log` rows for `account_id`, newest first — the
/// admin route's read.
///
/// `tx` must already be scoped appropriately for this table's SELECT policy
/// — see both migrations' headers: `tenancy::scope::begin_tenant_system_scoped`
/// on an ORG tenant pool (this table's tenant-plane SELECT policy is
/// `tenant_rls_is_system()`-only), or `tenancy::scope::begin_account_scoped`
/// on the consumer/control pool (control's SELECT policy is a plain
/// account-scope backstop). Either way, the caller
/// ([`crate::routes::query_audit::list_query_audit`]) must have ALREADY
/// verified `services::deletion::is_admin_role` before opening that
/// transaction — this function performs no admin check of its own (RLS
/// cannot see one; see the module doc's "SELECT/RLS split" section).
pub(crate) async fn list_page(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    account_id: Uuid,
    page_size: i64,
    offset: i64,
) -> Result<Vec<QueryAuditRow>, sqlx::Error> {
    sqlx::query_as::<_, QueryAuditRow>(
        "SELECT id, asked_by_user_id, question_digest, graphs_searched, atom_ids_cited, asked_at \
         FROM query_audit_log WHERE account_id = $1 ORDER BY asked_at DESC, id DESC LIMIT $2 OFFSET $3",
    )
    .bind(account_id)
    .bind(page_size)
    .bind(offset)
    .fetch_all(&mut **tx)
    .await
}

/// One `query_audit_log` row, as returned to an org admin.
#[derive(Debug, Clone, sqlx::FromRow, serde::Serialize)]
pub(crate) struct QueryAuditRow {
    pub id: Uuid,
    pub asked_by_user_id: Uuid,
    pub question_digest: String,
    pub graphs_searched: Vec<Uuid>,
    pub atom_ids_cited: Vec<Uuid>,
    pub asked_at: chrono::DateTime<chrono::Utc>,
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keyed_digest_is_64_hex_chars_and_key_dependent() {
        let d = digest_for_secret("what did we ship last week?", Some("test-key"));
        assert_eq!(d.len(), 64);
        assert!(d.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(
            d,
            digest_for_secret("what did we ship last week?", Some("other-key")),
            "different keys must produce different digests — that's the whole point"
        );
    }

    #[test]
    fn keyed_digest_is_stable_for_the_same_input_and_secret() {
        let a = digest_for_secret("same question", Some("test-key"));
        assert_eq!(a, digest_for_secret("same question", Some("test-key")));
        assert_ne!(a, digest_for_secret("same question.", Some("test-key")));
    }

    #[test]
    fn keyed_digest_never_uses_the_raw_secret_as_the_hmac_key() {
        // The raw secret also feeds OAuth state signing; a raw-keyed digest
        // over caller-chosen text is a state-forging oracle (see
        // `oauth::state`'s cross-purpose test for the end-to-end case).
        use hmac::{Hmac, Mac};
        let q = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ4In0";
        let mut raw = Hmac::<Sha256>::new_from_slice(b"test-key").unwrap();
        raw.update(q.as_bytes());
        let raw_digest = hex::encode(raw.finalize().into_bytes());
        assert_ne!(digest_for_secret(q, Some("test-key")), raw_digest);

        let key = crate::crypto::kdf::derive_key(
            b"test-key",
            crate::crypto::kdf::purpose::QUERY_AUDIT_DIGEST,
        );
        let mut derived = Hmac::<Sha256>::new_from_slice(&key).unwrap();
        derived.update(q.as_bytes());
        assert_eq!(
            digest_for_secret(q, Some("test-key")),
            hex::encode(derived.finalize().into_bytes())
        );
    }

    #[test]
    fn unkeyed_digest_is_visibly_prefixed() {
        let d = digest_for_secret("what did we ship last week?", None);
        let hexpart = d.strip_prefix("unkeyed:").expect("unkeyed digests carry the marker");
        assert_eq!(hexpart.len(), 64);
        assert!(hexpart.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn question_digest_env_wrapper_matches_the_check_constraint_shape() {
        // Whatever the ambient env holds, the stored value must satisfy the
        // migrations' `^(unkeyed:)?[0-9a-f]{64}$` CHECK.
        let d = question_digest("what did we ship last week?");
        let hexpart = d.strip_prefix("unkeyed:").unwrap_or(&d);
        assert_eq!(hexpart.len(), 64);
        assert!(hexpart.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn question_digest_is_deterministic() {
        assert_eq!(question_digest("same question"), question_digest("same question"));
    }

    #[test]
    fn question_digest_differs_for_different_questions() {
        assert_ne!(question_digest("question a"), question_digest("question b"));
    }

    #[test]
    fn question_digest_never_contains_the_source_text() {
        // Not a security proof (SHA-256 preimage resistance already provides
        // that) — just a sanity check that nothing upstream accidentally
        // swapped this for an encoding rather than a hash.
        let secret = "the launch date is confidential";
        let d = question_digest(secret);
        assert!(!d.contains("launch"));
        assert_ne!(d, secret);
    }

}
