//! FM-5 (`docs/plans/capture/02-b2-retract-fixes.md`, B2 unit 02, D5/D7/D8)
//! — the append-only write path over `retraction_audit_log`
//! (`migrations_control/0196_retraction_audit_log.sql` — see that
//! migration's own header for the full schema/RLS/REVOKE contract this
//! module writes against; nothing here re-derives that reasoning).
//!
//! # One pool, deliberately the CONTROL one
//!
//! `services::deletion`'s three entry points run against a CONTENT pool
//! that may be the shared consumer/control database OR a physically
//! separate ORG TENANT database (Neon) — see that module's own doc.
//! `retraction_audit_log` is CONTROL-PLANE ONLY, so [`record`] always takes
//! a SEPARATE, independently-resolved `control_pool` — the caller's
//! `AppState.db`/`state.db` at the route layer — never the `pool` deletion's
//! entry points themselves operate against. This is what makes the FM-4
//! `not_a_member` case writable at all: the caller has just lost membership
//! in the tenant DB the content lived in, but the control plane is
//! unaffected.
//!
//! `is_personal_ocean` (D8's author-only trigger) is NOT resolved here —
//! `services::deletion::delete_atom_cascade` already resolves it, inside its
//! own RLS-scoped transaction against the CONTENT pool, and carries it on
//! every [`SurfaceOutcome`] it produces (see that function's own comment on
//! why: `graphs` may live on a database this module's `control_pool` cannot
//! see at all).
//!
//! # Durability tier
//!
//! Mirrors `services::deletion::delete_source`'s own split for
//! `connector_audit_log`: [`record`] is the HARD bar — its failure
//! propagates, because an audited action whose audit row cannot be written
//! must not read as silently successful (D4: *"This is the precondition
//! that makes accepting FM-3 and FM-4 legitimate rather than hand-waving...
//! D4 additionally makes it a legal artifact."*). [`record_best_effort`] is
//! for `Actor::System`/`Actor::Webhook` callers (background propagation,
//! e.g. `connectors::google_gmail`/`google_calendar`'s deletion
//! propagation) that already treat their OWN failure as retriable — forcing
//! the hard bar there would be a regression, not a strengthening, exactly
//! `delete_source`'s own reasoning for `record_or_queue` vs `record`.
//!
//! ## PER-ATOM, not per-batch (findings 2/5, independent review)
//!
//! `services::deletion::run_cascade` calls [`record`]/[`record_best_effort`]
//! ONCE PER ATOM, immediately after that atom's own deletion transaction
//! commits — NOT once for the whole multi-atom batch after the loop
//! finishes. An earlier revision batched: a single failure partway through
//! a large `delete_subject`/`delete_source` call (e.g. an R2 error on atom
//! 120 of 200) propagated BEFORE any audit row was ever written, so the 119
//! atoms already, successfully, irreversibly deleted got ZERO audit trail —
//! permanently, since a retry re-resolves a SMALLER remaining set (per
//! `run_cascade`'s own doc) and those 119 atoms are simply gone, never
//! revisited. Per-atom writes bound that blast radius to AT MOST the one
//! atom whose own audit write is in flight when a control-DB blip hits.
//!
//! **Accepted residual, named rather than silently carried**: because the
//! atom's own deletion (content-plane pool) and its audit row (control-plane
//! pool, a SEPARATE database) can never share one transaction, a narrow race
//! remains — atom N's deletion commits, then ITS OWN audit write fails
//! (`record`'s hard bar propagates the error to the caller as a 503), and a
//! retry of the whole entry point finds atom N already gone
//! (`atom_was_present: false`) and therefore never re-attempts N's audit
//! write. That ONE atom's audit trail can be lost. This is the SAME class of
//! irreconcilable two-system commit-ordering trade-off
//! `services::deletion`'s own module doc already accepts for the
//! R2-vs-Postgres ordering (blob deleted, row commit, no cross-system
//! transaction there either) — proportionate here, not new. [`record`]'s
//! hard-bar failure is ALSO logged at `tracing::error!` with the full
//! outcome (surface/source identity) before it propagates, so even when the
//! structured row is lost, the fact and its details are not silently gone —
//! an operator can reconstruct/backfill from logs. What this fix DOES fully
//! close: the batch-scale loss (100+ atoms losing their trail over one
//! unrelated atom's blob error) — that is eliminated, not merely narrowed.

use sqlx::PgPool;
use uuid::Uuid;

use crate::services::connector_audit::Actor;
use crate::services::deletion::SurfaceOutcome;

fn actor_kind(actor: Actor) -> &'static str {
    match actor {
        Actor::User(_) => "user",
        Actor::System => "system",
        Actor::Webhook => "webhook",
    }
}

fn actor_user_id(actor: Actor) -> Option<Uuid> {
    match actor {
        Actor::User(uid) => Some(uid),
        Actor::System | Actor::Webhook => None,
    }
}

/// Write every row in `outcomes` under the CALLER-SUPPLIED `retraction_id`
/// — see `services::deletion::run_cascade`'s own doc comment (findings 2/5,
/// independent review) for why this is no longer minted in here: this
/// function is now called PER ATOM (immediately after that atom's own
/// deletion transaction commits), not once for a whole multi-atom batch, so
/// every atom in one logical retract call must be told the SAME id by its
/// caller to keep the D6 UI's "one `retraction_id` renders as one list"
/// promise across the whole batch. A no-op (still returns `Ok(())`) when
/// `outcomes` is empty.
pub async fn record(
    control_pool: &PgPool,
    account_id: Uuid,
    actor: Actor,
    retraction_id: Uuid,
    outcomes: &[SurfaceOutcome],
) -> Result<(), sqlx::Error> {
    if outcomes.is_empty() {
        return Ok(());
    }
    let result: Result<(), sqlx::Error> = async {
        // `retraction_audit_log`'s INSERT policy (migration 0196) is
        // `account_id = current_setting('app.account_id')::uuid AND
        // (actor_user_id IS NULL OR actor_user_id = app.user_id)` — a BARE
        // `control_pool.begin()` sets NEITHER GUC, which under the
        // non-BYPASSRLS `coralswarm_app` role means every INSERT either
        // hard-errors (`app.account_id`'s strict `current_setting`, no
        // `missing_ok`) or is silently refused by the WITH CHECK (a `User`
        // actor's real `actor_user_id` can never equal an unset
        // `app.user_id`). Same fix shape `services::connector_audit::record`
        // already uses for the IDENTICAL control-plane
        // `app.account_id`/`app.user_id` INSERT-check pattern on
        // `connector_audit_log` — `begin_tenant_scoped` despite its name is
        // plane-agnostic (it is `set_config` calls, not a tenant-DB
        // connection), so it is exactly as correct here as it is there.
        let mut tx = match actor {
            Actor::User(uid) => crate::tenancy::scope::begin_tenant_scoped(control_pool, account_id, uid)
                .await
                .map_err(|e| sqlx::Error::Protocol(format!("retraction_audit::record scoped tx: {e}")))?,
            Actor::System | Actor::Webhook => {
                crate::tenancy::scope::begin_account_scoped(control_pool, account_id)
                    .await
                    .map_err(|e| sqlx::Error::Protocol(format!("retraction_audit::record scoped tx: {e}")))?
            }
        };
        for o in outcomes {
            insert_one(&mut tx, retraction_id, account_id, actor, o).await?;
        }
        tx.commit().await
    }
    .await;
    // Findings 2/5 (independent review): log LOUDLY before this hard-bar
    // failure propagates and turns an ALREADY-COMMITTED deletion into what
    // the caller sees as a failed request — see this module's own doc
    // ("PER-ATOM, not per-batch") for the accepted residual this is the
    // mitigation for. Every field needed to reconstruct/backfill the row by
    // hand is in the log line, since the structured row itself may be lost.
    if let Err(e) = &result {
        tracing::error!(
            error = %e, %account_id, %retraction_id, ?actor,
            surfaces = ?outcomes.iter().map(|o| (o.graph_id, o.surface, o.outcome, o.source_type.as_str(), o.source_id.as_str())).collect::<Vec<_>>(),
            "retraction_audit: HARD-BAR write failed for an already-committed deletion — the              content is gone but its audit row is not written; reconstruct from this log line if              the row is never recovered"
        );
    }
    result
}

/// Best-effort sibling of [`record`] — logs and swallows a write failure
/// rather than failing the deletion it describes, which has ALREADY
/// committed by the time this runs (matches `connector_audit::
/// record_or_queue`'s posture, minus the retry queue: unlike that log, a
/// lost retraction-audit row does not affect anything the app itself later
/// reads back — see this module's doc for why `System`/`Webhook` callers get
/// this weaker tier instead of [`record`]'s hard bar).
///
/// **No PRODUCTION call site reaches this today** (review finding 9):
/// `connectors::google_gmail`/`google_calendar` both deliberately pass
/// `control_pool: None` to `services::deletion::delete_source` (see their
/// own code comments — FM-5 write scoped out of this PR for
/// connector-propagated deletes), so `services::deletion::
/// record_retraction_audit` returns before ever reaching this function in
/// practice. Exercised directly anyway, with a real `control_pool`, by
/// `backend/tests/b2_retract_fixes_pg.rs`'s
/// `finding_9_record_best_effort_is_actually_exercised` — the write path
/// itself is proven correct even though nothing wires it up yet.
pub async fn record_best_effort(
    control_pool: &PgPool,
    account_id: Uuid,
    actor: Actor,
    retraction_id: Uuid,
    outcomes: &[SurfaceOutcome],
) {
    if let Err(e) = record(control_pool, account_id, actor, retraction_id, outcomes).await {
        tracing::warn!(
            error = %e, %account_id, %retraction_id,
            "retraction_audit: best-effort write failed — the deletion it describes already committed"
        );
    }
}

async fn insert_one(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    retraction_id: Uuid,
    account_id: Uuid,
    actor: Actor,
    o: &SurfaceOutcome,
) -> Result<(), sqlx::Error> {
    // Metadata-only per D7/the migration's own `manifest` CHECK — the ONE
    // allowlisted key is `survivors`; an empty array is omitted entirely
    // (matches `{}` rather than `{"survivors": []}`) so the CHECK's
    // `manifest - ARRAY['survivors'] = '{}'` is satisfied identically
    // either way, and the common case (`outcome::REMOVED`, no survivors)
    // writes the smallest row.
    let manifest = if o.survivors.is_empty() {
        serde_json::json!({})
    } else {
        serde_json::json!({ "survivors": o.survivors })
    };
    sqlx::query(
        "INSERT INTO retraction_audit_log \
            (retraction_id, account_id, actor_kind, actor_user_id, source_type, \
             source_id, atom_id, graph_id, is_personal_ocean, surface, outcome, manifest) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
    )
    .bind(retraction_id)
    .bind(account_id)
    .bind(actor_kind(actor))
    .bind(actor_user_id(actor))
    .bind(&o.source_type)
    .bind(&o.source_id)
    .bind(o.atom_id)
    .bind(o.graph_id)
    .bind(o.is_personal_ocean)
    .bind(o.surface)
    .bind(o.outcome)
    .bind(manifest)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Not-a-member (FM-4)'s write path — provided AND, since review finding 9,
/// actually tested (D7's vocabulary requires the schema/CHECK to accept
/// `outcome = 'not_a_member'` as a real, exercised value, not merely a
/// string nobody ever inserts — an earlier revision made this exact claim
/// while NOTHING called this function anywhere; the claim is now made true
/// by `backend/tests/b2_retract_fixes_pg.rs`'s
/// `finding_9_record_not_a_member_is_actually_exercised`, which drives a
/// real row through the real constraints and reads it back) but
/// **deliberately NOT wired into `routes::deletion`'s live reachability
/// 404** in this PR. Reason, stated plainly rather than silently narrowed:
/// `require_reachable`'s 404 fires identically whether the caller genuinely
/// departed the ocean's membership (FM-4's actual shape) OR the graph id
/// never existed / was never reachable in the first place (a typo, a probe,
/// an ordinary unauthorized-access attempt) — see that route's own doc,
/// "an unreachable ocean" is INTENTIONALLY indistinguishable from a
/// nonexistent one. Writing a `not_a_member` row on every such 404 would
/// misrepresent probes/typos as ex-membership; correctly distinguishing the
/// two needs a departed-member check this route does not have today (e.g.
/// `org_membership_tombstones`) — genuinely new work, which D5 did not
/// scope FM-5 to include and which would be "fixing" FM-4's own accepted
/// shape by the back door. Recorded as an open item in this unit's PR body,
/// not silently dropped.
#[allow(clippy::too_many_arguments)]
pub async fn record_not_a_member(
    control_pool: &PgPool,
    account_id: Uuid,
    actor_user_id: Uuid,
    graph_id: Uuid,
    is_personal_ocean: bool,
    source_type: &str,
    source_id: &str,
    atom_id: Option<Uuid>,
) -> Option<Uuid> {
    let retraction_id = Uuid::new_v4();
    // Same scoped-transaction requirement as `record`'s own comment: a bare
    // pool query sets neither `app.account_id` nor `app.user_id`, and this
    // row's `actor_kind` is always `'user'` here.
    let result: Result<(), sqlx::Error> = async {
        let mut tx =
            crate::tenancy::scope::begin_tenant_scoped(control_pool, account_id, actor_user_id)
                .await
                .map_err(|e| sqlx::Error::Protocol(format!("retraction_audit::record_not_a_member scoped tx: {e}")))?;
        sqlx::query(
            "INSERT INTO retraction_audit_log \
                (retraction_id, account_id, actor_kind, actor_user_id, source_type, \
                 source_id, atom_id, graph_id, is_personal_ocean, surface, outcome, manifest) \
             VALUES ($1, $2, 'user', $3, $4, $5, $6, $7, $8, 'content', 'not_a_member', '{}'::jsonb)",
        )
        .bind(retraction_id)
        .bind(account_id)
        .bind(actor_user_id)
        .bind(source_type)
        .bind(source_id)
        .bind(atom_id)
        .bind(graph_id)
        .bind(is_personal_ocean)
        .execute(&mut *tx)
        .await?;
        tx.commit().await
    }
    .await;
    match result {
        Ok(_) => Some(retraction_id),
        Err(e) => {
            tracing::warn!(
                error = %e, %account_id, %graph_id,
                "retraction_audit: best-effort not_a_member write failed"
            );
            None
        }
    }
}

// ── Read path: the D6 UI (routed separately from B2 unit 02/#718) ──────────
//
// `docs/plans/capture/02-b2-retract-fixes.md`'s "D6 UI" section names this
// gap explicitly: the migration's own read-pattern comment
// (`idx_retraction_audit_log_retraction_id`) anticipates "every row from ONE
// retraction_id", but nothing in #718 read the table back. These are that
// thin read — no new authorization logic: `retraction_audit_log`'s SELECT
// policy (migration 0196) ALREADY encodes the full D8 visibility rule in
// SQL (author, every ocean, OR an admin of the ocean a row belongs to), so
// the caller only has to open the transaction with ITS OWN
// `app.account_id`/`app.user_id` GUCs (`begin_tenant_scoped`, same call
// [`record`] above already makes against this same control pool) and let
// RLS filter — unlike `routes::query_audit`, there is no route-level role
// gate to apply first, because there is nothing RLS can't already express.

/// One `retraction_audit_log` row, as read back for the D6 UI. Mirrors the
/// table's columns 1:1 (see migration 0196's header for what each one
/// means) — `manifest` is handed back whole rather than pre-parsed, so a
/// caller reads `survivors` out of it the same way `services::deletion`
/// writes it in.
///
/// **Deliberately no `serde::Serialize` derive** (independent review of PR
/// #729, finding 15, LOW) — every caller shapes its OWN JSON via
/// `routes::retractions::entry_json` rather than serializing this struct
/// directly, so a derived impl was dead weight nothing exercised.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct RetractionAuditRow {
    pub retraction_id: Uuid,
    pub retracted_at: chrono::DateTime<chrono::Utc>,
    pub actor_user_id: Option<Uuid>,
    pub source_type: String,
    pub source_id: String,
    pub atom_id: Option<Uuid>,
    pub graph_id: Uuid,
    pub is_personal_ocean: bool,
    pub surface: String,
    pub outcome: String,
    pub manifest: serde_json::Value,
}

/// Cap on how many rows ONE `retraction_id` group may contribute to a single
/// response (independent review of PR #729, finding 6, HIGH). Pagination
/// (`page_size`) bounds GROUPS, not rows: `delete_subject`/`delete_source`
/// write one row per (atom, ocean, surface) under ONE `retraction_id`, so a
/// 10,000-atom subject deletion is ONE group — without a per-group cap, a
/// single `GET /v1/retractions/{id}` (or one oversized group inside an
/// otherwise-small `page_size=20` page) could materialize 200k+ rows into a
/// `Vec`, a `HashMap`, and one JSON body. Deliberately generous — this is a
/// cost/DoS backstop, not a UI page size — and never silent: a group that
/// hits the cap is reported `"truncated": true` (`routes::retractions::
/// entry_json`) rather than rendering as a complete report that happens to
/// stop early.
pub const MAX_ROWS_PER_RETRACTION_GROUP: i64 = 500;

/// One page of DISTINCT `retraction_id` groups visible to the caller under
/// RLS, SCOPED TO `account_id` (independent review of PR #729, finding 5,
/// BLOCKING). `retraction_audit_log`'s SELECT policy (migration 0196) admits
/// an admin via `EXISTS (... am.account_id = retraction_audit_log.account_id
/// AND am.user_id = app.user_id AND am.role IN ('owner','admin') ...)` — that
/// clause is keyed on `app.user_id` ONLY, so a caller who administers TWO
/// accounts sees every account they administer blended together with
/// nothing telling them apart, regardless of which host the request arrived
/// on. This `WHERE account_id = $1` is the SAME belt-and-suspenders shape
/// `services::query_audit::list_page` already uses on top of its own RLS —
/// added here so RLS is the SECOND gate, never the only one. Newest group
/// first (`MIN(retracted_at)` per group — the per-atom write loop in
/// `services::deletion::run_cascade` means rows in the same group can carry
/// slightly different timestamps; the group's own moment is its earliest
/// row). `tx` must already be scoped via `begin_tenant_scoped` against the
/// CONTROL pool — see this module's own doc, "One pool, deliberately the
/// CONTROL one".
///
/// `unreached_only` is `RetractionHistory`'s "Not fully removed" filter
/// (the artboard's own header control, `docs/design/capture/
/// RetractionHistory.dc.html`) and it is deliberately a SERVER-side
/// `HAVING`, not a client-side filter over one page. Ticket 09 Q15=(c)'s
/// whole reason for a persisted surface is that `out_of_window` stays
/// FINDABLE; a filter applied to page 1 alone would answer "nothing left
/// behind" while page 3 still held an unreached copy — the
/// absence-renders-as-success shape this program keeps re-finding. `NOT
/// $4::bool OR bool_or(outcome <> 'removed')` keeps the unfiltered query
/// byte-identical in behaviour when the flag is off (`NOT true` short-
/// circuits the group predicate away), so the default path is unchanged.
pub async fn list_retraction_ids_page(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    account_id: Uuid,
    page_size: i64,
    offset: i64,
    unreached_only: bool,
) -> Result<Vec<Uuid>, sqlx::Error> {
    sqlx::query_scalar::<_, Uuid>(
        "SELECT retraction_id FROM retraction_audit_log WHERE account_id = $1 \
         GROUP BY retraction_id \
         HAVING NOT $4::bool OR bool_or(outcome <> 'removed') \
         ORDER BY MIN(retracted_at) DESC, retraction_id LIMIT $2 OFFSET $3",
    )
    .bind(account_id)
    .bind(page_size)
    .bind(offset)
    .bind(unreached_only)
    .fetch_all(&mut **tx)
    .await
}

/// Every visible row for a SET of `retraction_id`s, EACH GROUP independently
/// capped at [`MAX_ROWS_PER_RETRACTION_GROUP`] rows (finding 6) and the whole
/// query SCOPED TO `account_id` (finding 5, same reasoning as
/// [`list_retraction_ids_page`]) — the list route's second pass, after
/// [`list_retraction_ids_page`] picked which groups belong on this page.
/// Fetches one extra row per group (`rn <= cap + 1`) purely so the caller can
/// tell "exactly `cap` rows" apart from "more than `cap` rows, truncated" —
/// see [`MAX_ROWS_PER_RETRACTION_GROUP`]'s own doc. Oldest row first within
/// each group, `(surface, source_id)` as a DETERMINISTIC tiebreak (finding
/// 13, LOW) — `retracted_at` alone can tie within a group (the per-atom
/// write loop can write two rows in the same transaction-commit instant),
/// and an unordered tie previously meant `entry.rows[0]` in
/// `settings/retraction-history/page.tsx` could pick a DIFFERENT row on
/// every read of the same data. The window function's own `ORDER BY` is the
/// SAME tuple, so a truncated group's cap is applied to the identical
/// ordering it's displayed in, not a different one.
pub async fn rows_for_retraction_ids(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    account_id: Uuid,
    ids: &[Uuid],
) -> Result<Vec<RetractionAuditRow>, sqlx::Error> {
    sqlx::query_as::<_, RetractionAuditRow>(
        "WITH ranked AS ( \
            SELECT retraction_id, retracted_at, actor_user_id, source_type, source_id, \
                   atom_id, graph_id, is_personal_ocean, surface, outcome, manifest, \
                   ROW_NUMBER() OVER (PARTITION BY retraction_id ORDER BY retracted_at ASC, surface ASC, source_id ASC) AS rn \
            FROM retraction_audit_log \
            WHERE account_id = $1 AND retraction_id = ANY($2) \
         ) \
         SELECT retraction_id, retracted_at, actor_user_id, source_type, source_id, \
                atom_id, graph_id, is_personal_ocean, surface, outcome, manifest \
         FROM ranked WHERE rn <= $3 \
         ORDER BY retraction_id, retracted_at ASC, surface ASC, source_id ASC",
    )
    .bind(account_id)
    .bind(ids)
    .bind(MAX_ROWS_PER_RETRACTION_GROUP + 1)
    .fetch_all(&mut **tx)
    .await
}

/// Every visible row for exactly ONE `retraction_id` — the single-entry read
/// (`GET /v1/retractions/{id}`, `routes::retractions::get_retraction`), used
/// to re-hydrate the D6 post-retract report after the content it describes
/// may already be gone (the atom, or — FM-6 — the whole `coding_sessions`
/// row): this table (D7) is the only durable record left. An empty result
/// means either the id never existed, or RLS is hiding every row it has —
/// the route answers both cases identically (404), the same "an unreachable
/// ocean is intentionally indistinguishable from a nonexistent one" posture
/// `routes::deletion::require_reachable`'s own doc already accepts.
pub async fn rows_for_one_retraction(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    account_id: Uuid,
    retraction_id: Uuid,
) -> Result<Vec<RetractionAuditRow>, sqlx::Error> {
    rows_for_retraction_ids(tx, account_id, std::slice::from_ref(&retraction_id)).await
}

/// Every visible row whose `source_id` identifies ONE coding session —
/// either the session-level `"coding_session"`-surface row itself
/// (`source_id == session_id`, written by FM-6's
/// `delete_orphaned_coding_session`) or one of its checkpoints' own
/// `"content"`-surface rows (`source_id` under the `"cc:{session_id}:"`
/// prefix, [`crate::store::session_source_id_prefix`]) — ACROSS EVERY
/// `retraction_id` that ever touched this session, not only the one that
/// finally ended it.
///
/// Independent review of PR #729, finding 3, BLOCKING: retracting a
/// session's checkpoints one at a time mints one `retraction_id` PER
/// checkpoint, so the report shown when the session finally disappears
/// (`SessionRetracted`) must not describe only the last call — this is the
/// read that lets it describe all of them. `DELETE /v1/graphs/{id}/sources`
/// (`delete_source`) was considered and REJECTED as the fix here: its
/// `atoms.source_id = $1` match is EXACT, and a coding-session checkpoint's
/// `source_id` is unique PER CHECKPOINT (`"cc:{session_id}:{checkpoint_id}"`,
/// chunk-suffixed) — never shared across a session's checkpoints — so
/// calling it with any one session-scoped id would retract at most ONE
/// checkpoint's atoms, not "a whole session" under one id. (Confirmed: no
/// call site or test anywhere in this codebase invokes `delete_source` with
/// `source_type = "coding_session"`.) Reading the session's full retraction
/// history back out of the audit log — which already has every row, spread
/// across however many calls it took — is the fix that matches how sessions
/// are actually retracted today (checkpoint by checkpoint), not a change to
/// that interaction.
///
/// Same `account_id` scoping and [`MAX_ROWS_PER_RETRACTION_GROUP`] cap as
/// [`rows_for_retraction_ids`] — applied per `retraction_id` group WITHIN
/// this session's own row set, not globally.
pub async fn rows_for_session(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    account_id: Uuid,
    session_id: &str,
) -> Result<Vec<RetractionAuditRow>, sqlx::Error> {
    let prefix = crate::store::session_source_id_prefix(session_id);
    sqlx::query_as::<_, RetractionAuditRow>(
        "WITH ranked AS ( \
            SELECT retraction_id, retracted_at, actor_user_id, source_type, source_id, \
                   atom_id, graph_id, is_personal_ocean, surface, outcome, manifest, \
                   ROW_NUMBER() OVER (PARTITION BY retraction_id ORDER BY retracted_at ASC, surface ASC, source_id ASC) AS rn \
            FROM retraction_audit_log \
            WHERE account_id = $1 AND (source_id = $2 OR source_id LIKE $3) \
         ) \
         SELECT retraction_id, retracted_at, actor_user_id, source_type, source_id, \
                atom_id, graph_id, is_personal_ocean, surface, outcome, manifest \
         FROM ranked WHERE rn <= $4 \
         ORDER BY retraction_id, retracted_at ASC, surface ASC, source_id ASC",
    )
    .bind(account_id)
    .bind(session_id)
    .bind(&prefix)
    .bind(MAX_ROWS_PER_RETRACTION_GROUP + 1)
    .fetch_all(&mut **tx)
    .await
}
