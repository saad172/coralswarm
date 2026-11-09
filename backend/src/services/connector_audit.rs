//! Connector compliance audit log (m365 enterprise train PR 4.2, plan
//! decision #11 part 1; migrations `migrations_control/
//! 0193_connector_audit_log.sql` / `migrations_tenant/
//! 0071_connector_audit_log.sql`). Spec: "Append-only, tenant plane:
//! install/consent, container routing, seat grant/revoke, ingestion-mode
//! changes, disconnect, deletions — actor + timestamp."
//!
//! Sibling spec 4.3 (`query_audit_log`, `services::query_audit`, open PR
//! #697 at the time this module was written) is a SEPARATE table — the
//! DISCLOSURE log, with a deliberate durable-WARN posture: a lost row is an
//! accepted residual because the feature it backs (telling an org what its
//! members asked) is not itself a compliance control. THIS table is
//! different in kind: it *is* the compliance record for connector
//! lifecycle events, so losing a row silently is the failure this module
//! exists to prevent.
//!
//! # The durability contract — three tiers, chosen per call site
//!
//! 1. **[`record_in_tx`]** — write inside a transaction the caller already
//!    owns. Used when the audited action and its audit row must commit or
//!    abort together (e.g. writing `connector_installs` and its `install`
//!    audit row in the same transaction as `routes::oauth::callback`). A
//!    write failure here is an ordinary `sqlx::Error` the caller propagates
//!    with `?` — the transaction never commits, so the action itself never
//!    "succeeds" without its audit row.
//! 2. **[`record`]** — opens its own scoped transaction, writes, commits.
//!    For a request-path action whose primary state change is NOT
//!    conveniently in the same transaction as the audit write (a different
//!    pool/plane, or already committed by the time the audit point is
//!    reached) but where the HTTP response has not been sent yet: the
//!    caller `?`-propagates a failure into a 500, so no successful response
//!    is ever returned for an action this table failed to record. This is
//!    the HARD bar the spec calls for: **an audited action that cannot
//!    write its audit row must fail.**
//! 3. **[`record_or_queue`]** — for worker/reconciler/webhook paths that
//!    must NOT be allowed to fail the action they describe (a seat
//!    reconciler tick mid-sweep, a disconnect purge that already ran after
//!    the primary delete committed). Never silently drops: it retries
//!    inline with bounded backoff, and if every attempt fails it logs at
//!    `error!` (an incident-grade signal, not `warn!`) rather than merely
//!    warning. **No new job-queue `JobKind` was added for this PR** —
//!    disproportionate for a first cut given every caller of this function
//!    is already inside a periodic sweep (`workers::teams_seat_reconcile`,
//!    `workers::slack_seat_reconcile`) that re-asserts its state on every
//!    tick, or a webhook-driven propagation path whose own caller already
//!    treats a transient failure as retriable. A row lost after the bounded
//!    retries AND the next sweep/redelivery both fail is an accepted,
//!    narrow residual — documented here rather than silently assumed, per
//!    the PR spec's explicit escape valve for when a durable queue would be
//!    disproportionate.
//!
//! # Digest, not payload
//!
//! `subject` and `detail` are ids/counts ONLY, mirroring `mcp::audit` and
//! `services::query_audit`'s discipline — never a token, a message body, or
//! any other payload. [`AuditEntry`] bounds both defensively in Rust
//! ([`MAX_SUBJECT_LEN`], [`MAX_DETAIL_BYTES`]) ahead of the migrations' own
//! CHECK constraints, which are the hard backstop, not the primary guard —
//! an oversized value is truncated/dropped here rather than turning into a
//! failed compliance write.

use serde_json::Value;
use sqlx::{Acquire, PgPool};
use uuid::Uuid;

/// Who/what caused the audited action. Determines both `actor_kind`/
/// `actor_user_id` on the row and which RLS scope the write opens under.
/// `pub` (not `pub(crate)`, unlike the rest of this module): it is a
/// parameter type of `services::deletion::delete_source`, which
/// `backend/tests/deletion_pg.rs` calls directly from a separate
/// compilation unit — see this module's registration in `services::mod`
/// for the full reasoning.
#[derive(Debug, Clone, Copy)]
pub enum Actor {
    /// A real, gate-verified end user acting through their own session.
    User(Uuid),
    /// A background worker/reconciler acting tenant-wide, with no single
    /// member identity to present (mirrors `tenancy::scope::
    /// begin_tenant_system_scoped`'s own "no honest per-member identity"
    /// reasoning).
    System,
    /// An inbound provider webhook/event with no verified CoralSwarm user
    /// attached. Recorded distinctly from `System` (a different `actor_kind`
    /// value) even though both write under the same system-scoped
    /// transaction — there is no RLS-visible difference between the two,
    /// only a documentation one at the row level.
    Webhook,
}

impl Actor {
    fn user_id(self) -> Option<Uuid> {
        match self {
            Actor::User(uid) => Some(uid),
            Actor::System | Actor::Webhook => None,
        }
    }

    fn kind_str(self) -> &'static str {
        match self {
            Actor::User(_) => "user",
            Actor::System => "system",
            Actor::Webhook => "webhook",
        }
    }
}

/// Defensive ceiling on `subject`, ahead of the migrations'
/// `char_length(subject) <= 256` CHECK — truncated here (char-boundary
/// safe) rather than letting an oversized value fail the whole insert,
/// same "degrade, don't drop" reasoning as `mcp::audit::truncate_tool_name`.
const MAX_SUBJECT_LEN: usize = 256;
/// Defensive ceiling on `detail`'s serialized size, matching the migrations'
/// `octet_length(detail::text) <= 4096` CHECK EXACTLY — see [`jsonb_text_len`]
/// for why this can be the real DB limit rather than a padded-down guess
/// (CodeRabbit round 1 on #701: `serde_json::to_vec`'s compact byte count
/// used to be measured here, which undercounts relative to what the CHECK
/// actually measures — `jsonb::text`, which PostgreSQL renders with a space
/// after every structural `:` and `,` — so a compact value near 4096 could
/// pass this bound and still fail the CHECK on insert).
const MAX_DETAIL_BYTES: usize = 4096;

fn bound_subject(subject: Option<String>) -> Option<String> {
    subject.map(|s| {
        if s.chars().count() <= MAX_SUBJECT_LEN {
            s
        } else {
            s.chars().take(MAX_SUBJECT_LEN).collect()
        }
    })
}

/// A `serde_json::ser::Formatter` that renders JSON the way PostgreSQL's
/// `jsonb::text` cast does: a single ASCII space after every structural `:`
/// and `,` (`{"a": 1, "b": 2}`, not serde_json's compact `{"a":1,"b":2}`).
/// Every other byte serde_json's default `CompactFormatter` would write
/// (string escaping, number formatting, structural brackets) is identical —
/// this only overrides the three delimiter hooks that add jsonb's spacing.
///
/// Object-key ordering can differ from jsonb's own canonical key order (by
/// length then byte value — a `serde_json::Value::Object`'s default
/// `serde_json::Map` is insertion/BTreeMap ordered instead), but that only
/// changes WHERE bytes land, never how MANY there are — every key, value,
/// colon, comma, and space serde_json emits here also appears in jsonb's
/// text, so the total length matches regardless of order. Number formatting
/// is the one residual gap (e.g. `1.50` vs jsonb's `1.5`) — immaterial here
/// since `detail` is documented (module doc, "Digest, not payload") to hold
/// ids/counts only, never arbitrary floats.
struct JsonbTextFormatter;

impl serde_json::ser::Formatter for JsonbTextFormatter {
    fn begin_array_value<W: ?Sized + std::io::Write>(
        &mut self,
        writer: &mut W,
        first: bool,
    ) -> std::io::Result<()> {
        writer.write_all(if first { b"" } else { b", " })
    }

    fn begin_object_key<W: ?Sized + std::io::Write>(
        &mut self,
        writer: &mut W,
        first: bool,
    ) -> std::io::Result<()> {
        writer.write_all(if first { b"" } else { b", " })
    }

    fn begin_object_value<W: ?Sized + std::io::Write>(&mut self, writer: &mut W) -> std::io::Result<()> {
        writer.write_all(b": ")
    }
}

/// Byte length of `v` as PostgreSQL's `detail::text` cast would render it —
/// the SAME quantity the migrations' `octet_length(detail::text) <= 4096`
/// CHECK measures. See [`JsonbTextFormatter`] for the rendering contract and
/// its one documented residual gap.
fn jsonb_text_len(v: &Value) -> usize {
    let mut buf = Vec::new();
    let mut ser = serde_json::Serializer::with_formatter(&mut buf, JsonbTextFormatter);
    match serde::Serialize::serialize(v, &mut ser) {
        Ok(()) => buf.len(),
        // `Value`'s `Serialize` impl cannot fail — this is unreachable in
        // practice. Treat a hypothetical failure as "too big" (fail closed
        // into the elide path below) rather than "too small" (fail open
        // into an uncounted write).
        Err(_) => usize::MAX,
    }
}

/// Drops (rather than truncates — JSON truncated mid-structure is no
/// longer valid JSON) an oversized `detail` value, replacing it with a
/// marker so the row still records THAT detail was supplied and elided,
/// not merely absent.
fn bound_detail(detail: Option<Value>) -> Option<Value> {
    detail.map(|v| {
        let bytes = jsonb_text_len(&v);
        if bytes <= MAX_DETAIL_BYTES {
            v
        } else {
            tracing::warn!(bytes, "connector_audit: detail exceeded the size bound — elided");
            serde_json::json!({"_elided": true, "_original_bytes": bytes})
        }
    })
}

/// One `connector_audit_log` row to be written. `action`/`provider` are
/// `&'static str` — every call site names a literal from the migrations'
/// closed `action` CHECK vocabulary, never a caller-derived string.
pub(crate) struct AuditEntry {
    pub account_id: Uuid,
    pub actor: Actor,
    pub action: &'static str,
    /// A `String`, not `&'static str`, because several call sites (e.g.
    /// `routes::oauth::delete_connection`'s `provider` path parameter) only
    /// have a runtime-owned provider string in hand — forcing a `'static`
    /// bound there would mean leaking memory just to satisfy this type.
    pub provider: Option<String>,
    pub subject: Option<String>,
    pub detail: Option<Value>,
}

/// Write one row inside a transaction the caller already owns and will
/// commit itself. See the module doc's durability tier 1 — the caller must
/// scope `tx` appropriately for `entry.actor` BEFORE calling this:
/// `Actor::User(uid)` requires `tenancy::scope::set_tenant_user(tx, uid)` (or
/// the transaction to have been opened via `begin_tenant_scoped`) so the
/// tenant-plane `actor_user_id = tenant_rls_actor_user_id()` / control-plane
/// `actor_user_id = app.user_id` INSERT check is satisfiable;
/// `Actor::System`/`Actor::Webhook` require the transaction to carry
/// `app.tenant_actor = 'system'` (`begin_tenant_system_scoped`) on the
/// tenant plane — a bare `begin_account_scoped` transaction is NEVER
/// sufficient for a `System`/`Webhook` row on the tenant plane and will be
/// refused by RLS.
pub(crate) async fn record_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    entry: AuditEntry,
) -> Result<(), sqlx::Error> {
    let subject = bound_subject(entry.subject);
    let detail = bound_detail(entry.detail);
    sqlx::query(
        "INSERT INTO connector_audit_log \
            (account_id, actor_user_id, actor_kind, action, provider, subject, detail) \
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(entry.account_id)
    .bind(entry.actor.user_id())
    .bind(entry.actor.kind_str())
    .bind(entry.action)
    .bind(entry.provider)
    .bind(subject)
    .bind(detail)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Idempotent lifecycle event inside the caller's existing scoped transaction.
/// The caller derives `event_id` from a verified lifecycle identity, never from
/// user-supplied content. A savepoint catches only the deterministic primary-key
/// replay: ON CONFLICT cannot be used here because PostgreSQL additionally
/// applies the audit SELECT policy, which intentionally hides member rows.
pub(crate) async fn record_once_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    event_id: Uuid,
    entry: AuditEntry,
) -> Result<(), sqlx::Error> {
    let mut attempt = tx.begin().await?;
    let result = sqlx::query(
        "INSERT INTO connector_audit_log \
        (id,account_id,actor_user_id,actor_kind,action,provider,subject,detail) \
        VALUES($1,$2,$3,$4,$5,$6,$7,$8)",
    )
    .bind(event_id)
    .bind(entry.account_id)
    .bind(entry.actor.user_id())
    .bind(entry.actor.kind_str())
    .bind(entry.action)
    .bind(entry.provider)
    .bind(bound_subject(entry.subject))
    .bind(bound_detail(entry.detail))
    .execute(&mut *attempt)
    .await;
    match result {
        Ok(_) => attempt.commit().await,
        Err(sqlx::Error::Database(ref e))
            if e.code().as_deref() == Some("23505")
                && e.constraint() == Some("connector_audit_log_pkey") =>
        {
            attempt.rollback().await
        }
        Err(e) => {
            attempt.rollback().await?;
            Err(e)
        }
    }
}

/// Write one row in its own scoped transaction, opened and committed here.
/// See the module doc's durability tier 2 — a request-path caller
/// `?`-propagates a failure into a 500 so a successful response is never
/// returned for an action this table failed to record.
pub(crate) async fn record(pool: &PgPool, entry: AuditEntry) -> Result<(), sqlx::Error> {
    let account_id = entry.account_id;
    let mut tx = match entry.actor {
        Actor::User(uid) => crate::tenancy::scope::begin_tenant_scoped(pool, account_id, uid)
            .await
            .map_err(|e| sqlx::Error::Protocol(format!("connector_audit::record scoped tx: {e}")))?,
        Actor::System | Actor::Webhook => {
            crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id)
                .await
                .map_err(|e| sqlx::Error::Protocol(format!("connector_audit::record scoped tx: {e}")))?
        }
    };
    record_in_tx(&mut tx, entry).await?;
    tx.commit().await
}

/// Bounded backoff schedule for [`record_or_queue`]'s inline retries —
/// three attempts total (the first plus two retries), short enough that a
/// worker tick or a post-commit purge step is not meaningfully delayed by a
/// transient DB blip, long enough to ride out a brief connection hiccup.
const RETRY_BACKOFF: [std::time::Duration; 2] =
    [std::time::Duration::from_millis(200), std::time::Duration::from_millis(600)];

/// [`record`], but for a caller that must never fail the action it
/// describes (module doc, durability tier 3). Retries inline with the
/// [`RETRY_BACKOFF`] schedule; if every attempt fails, logs at `error!` —
/// an incident-grade signal a healthy-DB regression should page on, same
/// escalation `services::query_audit::record`'s own doc comment documents
/// for its analogous case — and returns without propagating anything to
/// the caller. Never a silent `warn!`-and-drop: every failure is loud, even
/// though it cannot block the caller.
pub(crate) async fn record_or_queue(pool: &PgPool, entry: AuditEntry) {
    let account_id = entry.account_id;
    let action = entry.action;
    let mut attempt = 0usize;
    loop {
        // `AuditEntry` is not `Clone` (its `Actor`/literals are cheap to
        // rebuild, but `subject`/`detail` are owned) — reconstruct a fresh
        // entry each attempt from the same source fields rather than
        // requiring callers to hand over a `Clone` bound just for this.
        let attempt_entry = AuditEntry {
            account_id: entry.account_id,
            actor: entry.actor,
            action: entry.action,
            provider: entry.provider.clone(),
            subject: entry.subject.clone(),
            detail: entry.detail.clone(),
        };
        match record(pool, attempt_entry).await {
            Ok(()) => return,
            Err(e) => {
                if attempt < RETRY_BACKOFF.len() {
                    tracing::warn!(
                        error = %e,
                        account_id = %account_id,
                        action = %action,
                        attempt,
                        "connector_audit_log write failed — retrying"
                    );
                    tokio::time::sleep(RETRY_BACKOFF[attempt]).await;
                    attempt += 1;
                    continue;
                }
                tracing::error!(
                    error = %e,
                    account_id = %account_id,
                    action = %action,
                    "connector_audit_log write failed after retries — this action will NOT \
                     appear in the compliance log; the underlying action was NOT rolled back \
                     (this is the documented, narrow residual for worker/webhook paths — see \
                     services::connector_audit's module doc)"
                );
                return;
            }
        }
    }
}

/// One `connector_audit_log` row, as returned to an org admin.
#[derive(Debug, Clone, sqlx::FromRow, serde::Serialize)]
pub(crate) struct ConnectorAuditRow {
    pub id: Uuid,
    pub actor_user_id: Option<Uuid>,
    pub actor_kind: String,
    pub action: String,
    pub provider: Option<String>,
    pub subject: Option<String>,
    pub detail: Option<Value>,
    pub occurred_at: chrono::DateTime<chrono::Utc>,
}

/// One page of `connector_audit_log` rows for `account_id`, newest first —
/// the admin route's read. `tx` must already be scoped for this table's
/// SELECT policy (system-scoped on an org tenant pool; account-scoped on
/// control) — same contract as `services::query_audit::list_page`, and the
/// same caveat: the caller MUST already have verified
/// `services::deletion::is_admin_role` before opening that transaction.
pub(crate) async fn list_page(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    account_id: Uuid,
    page_size: i64,
    offset: i64,
) -> Result<Vec<ConnectorAuditRow>, sqlx::Error> {
    sqlx::query_as::<_, ConnectorAuditRow>(&format!(
        "SELECT {AUDIT_ROW_COLUMNS} \
         FROM connector_audit_log WHERE account_id = $1 \
         {AUDIT_ROW_ORDER} LIMIT $2 OFFSET $3",
    ))
    .bind(account_id)
    .bind(page_size)
    .bind(offset)
    .fetch_all(&mut **tx)
    .await
}

/// The one column list + order both page readers share (CodeRabbit round 2
/// on #701): [`ConnectorAuditRow`] decodes positionally via `query_as`, so
/// if [`list_page`] and [`list_page_after`] ever selected different columns
/// (or different orders), the same endpoint would return different shapes
/// depending on whether a cursor was passed — and the drifted path would
/// fail to decode at runtime. One fragment makes that drift impossible.
const AUDIT_ROW_COLUMNS: &str =
    "id, actor_user_id, actor_kind, action, provider, subject, detail, occurred_at";
const AUDIT_ROW_ORDER: &str = "ORDER BY occurred_at DESC, id DESC";

/// Keyset page: rows strictly older than the `(occurred_at, id)` cursor the
/// previous page ended on — `None` for the FIRST page (equivalent to
/// `list_page`'s `offset = 0`).
///
/// CodeRabbit round 1 on #701: [`list_page`]'s `LIMIT ... OFFSET` is stable
/// only against a frozen table. `connector_audit_log` is append-only and
/// read newest-first, so a write landing between two calls to `list_page`
/// shifts the whole window — an admin walking pages can see a row twice
/// (re-shown on the next page) or never (skipped past the boundary). For a
/// compliance-log export that is a silent gap, not a cosmetic one.
///
/// This function is immune to that: `(occurred_at, id) < (cursor_at,
/// cursor_id)` names an absolute position in the newest-first order rather
/// than a row COUNT from the top, so a row inserted after the cursor was
/// issued can never appear on a later page (it sorts before the cursor) and
/// a row already returned can never reappear (it sorts after). Uses the
/// same `idx_connector_audit_log_account_occurred (account_id, occurred_at
/// DESC, id DESC)` index `list_page` does — the tuple comparison is exactly
/// what that index supports.
///
/// `routes::connector_audit::list_connector_audit` keeps [`list_page`]
/// (OFFSET) as the page 1 / no-cursor path — see that route's own doc
/// comment for why full backward compatibility was worth keeping rather
/// than replacing it outright.
pub(crate) async fn list_page_after(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    account_id: Uuid,
    page_size: i64,
    cursor: Option<(chrono::DateTime<chrono::Utc>, Uuid)>,
) -> Result<Vec<ConnectorAuditRow>, sqlx::Error> {
    let (cursor_at, cursor_id) = match cursor {
        Some((at, id)) => (Some(at), Some(id)),
        None => (None, None),
    };
    sqlx::query_as::<_, ConnectorAuditRow>(&format!(
        "SELECT {AUDIT_ROW_COLUMNS} \
         FROM connector_audit_log WHERE account_id = $1 \
           AND ($2::timestamptz IS NULL OR (occurred_at, id) < ($2, $3)) \
         {AUDIT_ROW_ORDER} LIMIT $4",
    ))
    .bind(account_id)
    .bind(cursor_at)
    .bind(cursor_id)
    .bind(page_size)
    .fetch_all(&mut **tx)
    .await
}

/// Best-effort provider classification from a free-form `source_type`
/// string (`services::deletion::delete_source`'s own parameter — arbitrary
/// text a caller supplies, e.g. `"slack"`/`"google_calendar"`/
/// `"teams_message"`), for the `deletion_propagated` emit point's
/// `provider` field. Not a strict mapping — deliberately permissive
/// substring matches, since this is a compliance-log ENRICHMENT field, not
/// an authorization input: a misclassified provider costs a slightly wrong
/// filter value in the audit log, never a security decision.
pub(crate) fn provider_from_source_type(source_type: &str) -> &'static str {
    let s = source_type.to_ascii_lowercase();
    if s.contains("slack") {
        "slack"
    } else if s.contains("microsoft") || s.contains("teams") || s.contains("outlook") {
        "microsoft"
    } else if s.contains("google") || s.contains("gmail") || s.contains("calendar") {
        "google"
    } else {
        "other"
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn actor_user_kind_and_id_agree() {
        let uid = Uuid::new_v4();
        assert_eq!(Actor::User(uid).kind_str(), "user");
        assert_eq!(Actor::User(uid).user_id(), Some(uid));
        assert_eq!(Actor::System.kind_str(), "system");
        assert_eq!(Actor::System.user_id(), None);
        assert_eq!(Actor::Webhook.kind_str(), "webhook");
        assert_eq!(Actor::Webhook.user_id(), None);
    }

    #[test]
    fn bound_subject_leaves_short_values_untouched() {
        assert_eq!(bound_subject(Some("C123".to_string())), Some("C123".to_string()));
        assert_eq!(bound_subject(None), None);
    }

    #[test]
    fn bound_subject_truncates_at_the_char_boundary() {
        let hostile = "é".repeat(10_000);
        let bounded = bound_subject(Some(hostile)).unwrap();
        assert_eq!(bounded.chars().count(), MAX_SUBJECT_LEN);
    }

    #[test]
    fn bound_detail_leaves_small_values_untouched() {
        let v = serde_json::json!({"count": 3});
        assert_eq!(bound_detail(Some(v.clone())), Some(v));
        assert_eq!(bound_detail(None), None);
    }

    #[test]
    fn bound_detail_elides_oversized_values_rather_than_failing() {
        let huge = serde_json::json!({"ids": vec!["x".repeat(100); 200]});
        let bounded = bound_detail(Some(huge)).unwrap();
        assert_eq!(bounded["_elided"], serde_json::Value::Bool(true));
    }

    /// CodeRabbit round 1 on #701: pins [`jsonb_text_len`] against literal,
    /// hand-counted byte strings for both structural cases (object AND
    /// array — the two `Formatter` hooks this module overrides) so a future
    /// edit to [`JsonbTextFormatter`] that drops the space after `:` or `,`
    /// fails a test instead of only being visible as a production CHECK
    /// violation.
    #[test]
    fn jsonb_text_len_matches_postgres_jsonb_text_spacing() {
        // `{"count": 3}` — 12 bytes: compact `{"count":3}` (11 bytes) plus
        // the one space jsonb::text adds after `:`.
        assert_eq!(jsonb_text_len(&serde_json::json!({"count": 3})), 12);
        // `{"a": 1, "b": 2}` — 16 bytes: compact `{"a":1,"b":2}` (13 bytes)
        // plus one space after each of the two `:` and the one `,` (3).
        assert_eq!(jsonb_text_len(&serde_json::json!({"a": 1, "b": 2})), 16);
        // `[1, 2, 3]` — 9 bytes: compact `[1,2,3]` (7 bytes) plus one space
        // after each of the two `,`.
        assert_eq!(jsonb_text_len(&serde_json::json!([1, 2, 3])), 9);
        // No structural delimiters at all — same length either way.
        assert_eq!(jsonb_text_len(&serde_json::json!("plain")), 7);
    }

    /// The bug CodeRabbit round 1 on #701 found: `serde_json::to_vec`'s
    /// COMPACT byte count can sit at or under `MAX_DETAIL_BYTES` while the
    /// migrations' `octet_length(detail::text) <= 4096` CHECK — measured
    /// against jsonb's SPACED rendering — would still reject the same
    /// value. Builds a value whose compact form is comfortably under 4096
    /// but whose jsonb-text form exceeds it, and proves `bound_detail` now
    /// elides it (using the same measure the DB CHECK uses) rather than
    /// passing it through to an INSERT that would fail. The companion
    /// proof that a value `bound_detail` DOES pass through genuinely
    /// inserts against a real CHECK constraint is
    /// `connector_audit_pg_tests::record_accepts_a_detail_value_at_the_
    /// jsonb_text_boundary_pg` (`routes::connector_audit`, live Postgres).
    #[test]
    fn bound_detail_elides_a_value_the_old_compact_measure_would_have_missed() {
        // 372 short, densely-packed entries (4-char key, 1-char value) — a
        // shape maximizing the delimiter-to-payload ratio, the same class
        // of PoC the CodeRabbit finding itself used. Compact bytes
        // (11*372+1 = 4093) stay just under MAX_DETAIL_BYTES; jsonb-text
        // bytes add one space per `:` (372) and per non-first `,` (371) =
        // 743 more, landing at 4836 — well past the real 4096 DB limit.
        let mut map = serde_json::Map::new();
        for i in 0..372 {
            map.insert(format!("k{i:03}"), serde_json::Value::String("x".to_string()));
        }
        let v = serde_json::Value::Object(map);
        let compact = serde_json::to_vec(&v).unwrap().len();
        let rendered = jsonb_text_len(&v);
        assert!(compact <= MAX_DETAIL_BYTES, "fixture must reproduce the old measure's blind spot — compact was {compact}");
        assert!(rendered > MAX_DETAIL_BYTES, "fixture must exceed the real DB limit once rendered — got {rendered}");

        let bounded = bound_detail(Some(v)).unwrap();
        assert_eq!(
            bounded["_elided"], serde_json::Value::Bool(true),
            "a value whose COMPACT size passes but whose JSONB-TEXT size does not must still be elided"
        );
    }

    #[test]
    fn provider_from_source_type_classifies_known_prefixes() {
        assert_eq!(provider_from_source_type("slack"), "slack");
        assert_eq!(provider_from_source_type("google_calendar"), "google");
        assert_eq!(provider_from_source_type("gmail"), "google");
        assert_eq!(provider_from_source_type("teams_message"), "microsoft");
        assert_eq!(provider_from_source_type("outlook_thread"), "microsoft");
        assert_eq!(provider_from_source_type("notion"), "other");
    }
}
