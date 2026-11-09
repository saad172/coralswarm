//! The join-request-queue admin digest — CS-438, org-onboarding plan §3
//! item 5, D13 (`docs/plans/org-onboarding-from-the-invite.md` ~line 514).
//!
//! # The failure this closes
//!
//! CS-437 (item 4/14) built the join-request queue and the requester-facing
//! "your request has been sent to your admin" sentence. D13's own line:
//! "If the notification is not built, 'sent to your admin' should not be a
//! sentence we show anyone." Before this worker there was no notification
//! machinery of any kind — no digest, no inbox, no badge — so an admin who
//! never happens to open the members page would never learn a request
//! exists at all. This worker is the email half of making that sentence
//! true; `routes::org_join_requests::count`/the members-page badge (same
//! story) is the in-app half.
//!
//! # D13, amended 2026-09-22 — the digest also reports AUTO-ADDS
//!
//! CS-447 (item 9) made auto-add real. An auto-added member never creates
//! a join request, so before this amendment nothing would have told an
//! admin it happened — they would have had to notice a new name and a
//! spent slot. The founder's ruling: **the existing daily digest gains a
//! second, separate section** listing who was added automatically since the
//! last digest, and **which rung they qualified on**.
//!
//! Three things about that are load-bearing, and all three are "reuse, do
//! not fork":
//!
//!   * **The idempotency is unchanged.** `(account_id, admin_user_id,
//!     sent_on)` — migration 0240's PRIMARY KEY — **is** the once-a-day
//!     rule, and it now covers both sections. Auto-adds get no claim, no
//!     table and no cadence of their own; a second notification channel
//!     would be a second thing to get wrong on a day someone was both
//!     asked about AND added.
//!   * **The claim-then-send-then-commit order is unchanged**, so a failed
//!     send still never consumes the claim — for either section.
//!   * **The two sections stay visually distinct** (the founder's words:
//!     *"one is asking for a decision, the other is reporting a fact"*).
//!     [`render_digest_email_html`] renders them under separate eyebrow
//!     labels with different verbs, and the auto-add rows carry the rung
//!     that admitted each person.
//!
//! The one thing that had to change in the schema is 0240's
//! `CHECK (request_count > 0)`: a digest sent because three people were
//! auto-added and nobody asked has `request_count = 0`. Migration 0247
//! re-states that invariant over BOTH counts
//! (`request_count + auto_add_count > 0`) rather than dropping it — an
//! empty digest is still unrepresentable.
//!
//! # D13's rule, exactly
//!
//! **One email per admin per day, only on days with new requests.**
//! Batched, because a busy workspace would otherwise flood an inbox with
//! one email per request. Only on days with changes, because — D13's own
//! words — "an empty digest trains people to ignore the full one."
//!
//! # What "new" means, and where the idempotency lives
//!
//! `request_count` on a digest is never the total open queue size — it is
//! the count of requests that arrived STRICTLY AFTER this admin's own most
//! recent digest `sent_at` (any earlier day, not just yesterday). A three-
//! person queue reported in full on day one is reported as ONE new person
//! on day two if exactly one more person asked in between; the two people
//! already reported are never repeated. [`candidate_admins`] computes this
//! per admin with `COALESCE(MAX(sent_at), '-infinity')`, then re-reads the
//! actual NEW rows (for the names) with the same cutoff.
//!
//! "Already sent today" is checked FIRST and separately from "any new
//! requests" — the two skip counters
//! ([`Outcome::digests_skipped_already_today`] /
//! [`Outcome::digests_skipped_no_new`]) exist because they are genuinely
//! different conditions a caller (this module's own pg suite) needs to
//! distinguish: a second tick the same UTC day sends nothing because a row
//! for `(account_id, admin_user_id, sent_on)` already exists (idempotency,
//! `migration 0240`'s own PRIMARY KEY), even though a fresh `new_since`
//! computation against that row's own `sent_at` would ALSO read zero new
//! requests once the count already reported has passed by. Checking the
//! day first, then the count, keeps those two skip reasons honest rather
//! than collapsing them into one.
//!
//! `sent_on` is a UTC calendar date — [`today`] wraps `Utc::now().date_naive()`
//! once, named, rather than an inline call at each of the two SQL sites that
//! need it, so there is exactly one place this worker's notion of "today"
//! is defined. A tick that straddles UTC midnight can therefore send a
//! digest for the day it started in even if it finishes after midnight;
//! this worker's own `MAX_ACCOUNTS_PER_TICK`-bounded pass and
//! [`crate::workers::org_join_request_digest::digest_interval`]'s 15-minute
//! default cadence make that window negligible in practice, and the PK's
//! own idempotency means a borderline tick never double-sends either way —
//! it simply lands on whichever side of midnight it happened to run.
//!
//! # Claim vs send order — same divergence as `member_connect_nudge`
//!
//! Like `workers::member_connect_nudge` (see that module's own "Claim vs
//! send order" section), this worker holds the digest-row INSERT's
//! transaction OPEN across the Resend call and commits ONLY on a successful
//! send — a failed send rolls the claim back, so the very next tick treats
//! this admin as never having been told today and retries. The alternative
//! (claim-then-commit-then-send, `workers::slack_unmatched_email`'s own
//! order) would durably mark "digest sent" against an email that never
//! went out — for a once-a-day notification that is a full day of silence,
//! not a merely-deferred retry, so the same trade `member_connect_nudge`
//! already made for its own per-recipient cooldown applies here too.
//!
//! # Shape: `tenant_harness` sweep worker, always control-plane
//!
//! Every table this worker reads or writes — `accounts`, `account_members`,
//! `users`, `personal_orgs`, `org_join_requests`, `org_join_request_digests`
//! — is control-plane-only (`scripts/check_schema_parity.sh`'s
//! `CONTROL_TABLES`), so exactly like `member_connect_nudge`, `t.pool` is
//! never touched for anything but identifying which org this tick is for;
//! `control` (a separately-captured clone of the control pool) is the only
//! pool [`tick`]/[`process_account`] ever read or write. Registers as a
//! [`crate::workers::tenant_harness::HarnessConfig::sweep_worker`], same as
//! every other control-plane-only sweep in this directory.
//!
//! Handle RETAINED and drained at shutdown (`lib.rs`'s own registration
//! comment gives the reasoning verbatim for `member_connect_nudge`; it
//! applies identically here): this worker has the same real external side
//! effect — a claim held open across a Resend call — so a hard kill in the
//! narrow window after Resend accepts the email but before the commit lands
//! would otherwise duplicate-send on the very next tick.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, NaiveDate, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use crate::clerk_admin::ClerkAdminClient;
use crate::services::org_auto_add as auto_add;
use crate::tenancy::scope::begin_tenant_system_scoped;
use crate::tenancy::TenantId;
use crate::workers::billing_derivation::resolve_org_account_id;
use crate::workers::tenant_harness::{Tick, WorkerTenant};
use crate::AppState;

/// Sweep cadence — same `_INTERVAL_SECS` env convention as every other
/// sweep worker in this directory (`billing_derivation::derivation_interval`,
/// `member_connect_nudge::nudge_interval`, …).
pub const DEFAULT_INTERVAL: Duration = Duration::from_secs(15 * 60);

pub fn digest_interval() -> Duration {
    std::env::var("JOIN_REQUEST_DIGEST_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_INTERVAL)
}

/// Hard per-tick cap on how many admins get a claim-and-send pass in one
/// account's tick — same bounding reasoning as `member_connect_nudge::
/// MAX_CANDIDATES_PER_TICK`: a claim is held open across a Resend call, so
/// an unbounded loop could hold a transaction for a long tail. A sweep
/// worker has no queue to enqueue a continuation onto — any admin left
/// untouched this tick is still a candidate on the NEXT one,
/// [`digest_interval`] later, since nothing about their eligibility changes
/// in between. In practice an org's admin roster is small (single digits),
/// so this cap is headroom, not an expected limit.
const MAX_ADMINS_PER_TICK: usize = 200;

/// "Today", UTC — the one place this worker's notion of a calendar day is
/// defined (module doc's own section). `NaiveDate` rather than
/// `DateTime<Utc>`: `org_join_request_digests.sent_on` is a bare `DATE`
/// column, and binding a `NaiveDate` keeps the Rust type and the SQL column
/// type matched instead of relying on a driver-level truncation.
fn today() -> NaiveDate {
    Utc::now().date_naive()
}

/// One admin who might be due a digest for `account_id` this tick — every
/// fact computed by [`candidate_admins`]'s own query.
struct AdminCandidate {
    admin_user_id: Uuid,
    email: String,
}

/// The account's admin roster (`account_members.role IN ('owner','admin')`,
/// migration `0001`'s own CHECK), joined to `users` for the email address —
/// same join shape `workers::member_connect_nudge::candidates_for_account`
/// uses for its own roster read, including the `personal_orgs` anti-join
/// (defense in depth: a personal org never reaches this worker in
/// production at all — see that sibling function's own doc comment for why
/// — but a caller handing this function a personal org's `account_id`
/// directly, this module's own pg suite included, still gets zero rows back
/// rather than silently emailing a "member" who is really just that org's
/// solo owner).
async fn candidate_admins(control: &PgPool, account_id: Uuid) -> Result<Vec<AdminCandidate>, String> {
    let mut tx = begin_tenant_system_scoped(control, account_id)
        .await
        .map_err(|e| format!("org_join_request_digest: failed to open scoped read transaction: {e}"))?;
    let rows: Vec<(Uuid, String)> = sqlx::query_as(
        "SELECT am.user_id, u.email \
         FROM account_members am \
         JOIN users u ON u.id = am.user_id \
         JOIN accounts a ON a.id = am.account_id \
         WHERE am.account_id = $1 \
           AND am.role IN ('owner', 'admin') \
           AND a.clerk_org_id IS NOT NULL \
           AND NOT EXISTS ( \
               SELECT 1 FROM personal_orgs po WHERE po.clerk_org_id = a.clerk_org_id \
           ) \
         ORDER BY am.joined_at ASC \
         LIMIT $2",
    )
    .bind(account_id)
    .bind(MAX_ADMINS_PER_TICK as i64)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| format!("org_join_request_digest: admin roster query failed: {e}"))?;
    tx.rollback().await.ok();

    Ok(rows
        .into_iter()
        .map(|(admin_user_id, email)| AdminCandidate { admin_user_id, email })
        .collect())
}

/// Any OPEN request at all for `account_id` — the account-level early-out
/// D13's own "for each ordinary org WITH open requests" wording asks for,
/// so a quiet org's tick never even queries its admin roster. A personal
/// org with a (never-production, test-only-direct-insert) open row still
/// reads `true` here; [`candidate_admins`]'s own `personal_orgs` anti-join
/// is what actually excludes it — this check is purely a cheap skip, not a
/// second place D19 is enforced.
///
/// **CS-447: no longer the whole early-out.** An org can now have nothing
/// in the queue and still owe its admins a digest, because somebody was
/// auto-added — see [`has_unreported_auto_adds`]. [`process_account`]
/// skips only when BOTH are false, and getting that wrong would have made
/// the auto-add section silently unreachable for exactly the orgs it
/// matters most to: the ones where auto-add is working and nobody has to
/// ask.
async fn has_open_requests(control: &PgPool, account_id: Uuid) -> Result<bool, String> {
    let mut tx = begin_tenant_system_scoped(control, account_id)
        .await
        .map_err(|e| format!("org_join_request_digest: failed to open scoped read transaction: {e}"))?;
    let any: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM org_join_requests WHERE account_id = $1 AND decision IS NULL)",
    )
    .bind(account_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| format!("org_join_request_digest: open-request existence check failed: {e}"))?;
    tx.rollback().await.ok();
    Ok(any)
}

/// Any auto-add for `account_id` that NO admin has been told about yet —
/// the auto-add half of the account-level early-out (CS-447, D13 as
/// amended 2026-09-22).
///
/// # Why this is not simply `EXISTS (SELECT 1 FROM org_auto_adds …)`
///
/// [`has_open_requests`] can be unbounded because an open request stops
/// being open once it is decided, so that early-out self-clears. An
/// `org_auto_adds` row is permanent — it is a ledger, not a queue — so an
/// unbounded existence check would mean that every org which has EVER
/// auto-added anybody runs the full per-admin pass on every tick, forever,
/// for a fact that was reported months ago.
///
/// The bound is the exact lower edge of every window [`process_account`]
/// will actually evaluate: the MINIMUM, across this account's admins, of
/// that admin's own last digest `sent_at` — with `-infinity` for an admin
/// who has never been sent one, which is why the `COALESCE` sits INSIDE the
/// per-admin grouping and not outside the `MIN`. (`MIN` skips NULLs, so
/// coalescing after the aggregate would ignore precisely the never-digested
/// admin whose window is widest, and skip an org that owes somebody their
/// first digest.)
///
/// So: never skips an auto-add any admin is still owed, and stops firing
/// once every admin has been told.
async fn has_unreported_auto_adds(control: &PgPool, account_id: Uuid) -> Result<bool, String> {
    let mut tx = begin_tenant_system_scoped(control, account_id)
        .await
        .map_err(|e| format!("org_join_request_digest: failed to open scoped read transaction: {e}"))?;
    let any: bool = sqlx::query_scalar(
        "SELECT EXISTS ( \
             SELECT 1 FROM org_auto_adds a \
              WHERE a.account_id = $1 \
                AND a.added_at > COALESCE(( \
                    SELECT MIN(per_admin.last_sent) FROM ( \
                        SELECT COALESCE(MAX(g.sent_at), '-infinity'::timestamptz) AS last_sent \
                          FROM account_members am \
                          LEFT JOIN org_join_request_digests g \
                            ON g.account_id = am.account_id AND g.admin_user_id = am.user_id \
                         WHERE am.account_id = $1 AND am.role IN ('owner', 'admin') \
                         GROUP BY am.user_id \
                    ) per_admin \
                ), '-infinity'::timestamptz) \
         )",
    )
    .bind(account_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| format!("org_join_request_digest: unreported-auto-add existence check failed: {e}"))?;
    tx.rollback().await.ok();
    Ok(any)
}

/// One person auto-added since an admin's last digest — D13's amendment
/// asks for the name AND the rung ("who was added automatically since the
/// last one, and which rung they qualified on").
struct AutoAddReport {
    name: Option<String>,
    /// `org_auto_adds.rung` — `"directory"` or `"member_and_domain"`, the
    /// D16 ladder's own labels (`assertion_ladder::Rung::label`). Rendered
    /// for a human by [`rung_label`], never shown raw.
    rung: String,
}

/// The auto-adds that make up ONE admin's digest section this tick —
/// exactly the same `since` cutoff, computed the same way, as
/// [`new_requesters_since`]. One cutoff per admin, two sections, so a
/// digest can never repeat a fact it has already reported in either of
/// them.
///
/// Callers that will write `sent_at` MUST run this on the SAME transaction
/// that already holds [`auto_add::lock_auto_add_digest`]. A separate
/// unlocked read can miss an in-flight INSERT whose `added_at` then loses
/// to the claim's `sent_at` forever.
async fn new_auto_adds_since_on(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    account_id: Uuid,
    since: Option<DateTime<Utc>>,
) -> Result<Vec<AutoAddReport>, String> {
    let rows: Vec<(Option<String>, String)> = sqlx::query_as(
        "SELECT u.name, a.rung \
         FROM org_auto_adds a \
         LEFT JOIN users u ON u.id = a.user_id \
         WHERE a.account_id = $1 \
           AND a.added_at > COALESCE($2, '-infinity'::timestamptz) \
         ORDER BY a.added_at ASC",
    )
    .bind(account_id)
    .bind(since)
    .fetch_all(&mut **tx)
    .await
    .map_err(|e| format!("org_join_request_digest: new-auto-add query failed: {e}"))?;
    Ok(rows.into_iter().map(|(name, rung)| AutoAddReport { name, rung }).collect())
}

/// Unlocked preview used only to skip a quiet admin without opening the
/// claim transaction. A concurrent in-flight INSERT that this misses is
/// fine: we do not write `sent_at` on this path, so the next tick still
/// sees the row. The claim path MUST re-read via [`new_auto_adds_since_on`]
/// under [`auto_add::lock_auto_add_digest`].
async fn new_auto_adds_since(
    control: &PgPool,
    account_id: Uuid,
    since: Option<DateTime<Utc>>,
) -> Result<Vec<AutoAddReport>, String> {
    let mut tx = begin_tenant_system_scoped(control, account_id)
        .await
        .map_err(|e| format!("org_join_request_digest: failed to open scoped read transaction: {e}"))?;
    let rows = new_auto_adds_since_on(&mut tx, account_id, since).await?;
    tx.rollback().await.ok();
    Ok(rows)
}

/// A NEW open requester for one admin's digest — arrived strictly after
/// `since` (that admin's own last digest `sent_at`, or `-infinity` if they
/// have never been sent one).
struct NewRequester {
    #[allow(dead_code)] // carried for a future "who" click-through; unused today.
    user_id: Uuid,
    name: Option<String>,
}

/// The requests that make up ONE admin's digest this tick — every open
/// request for `account_id` requested strictly after `since`, oldest
/// first. An empty result IS the "no new requests" signal
/// [`process_account`] reads to skip sending — see the module doc's "What
/// 'new' means" section for why this is computed fresh per admin (each
/// admin has their own `since`, not a shared account-wide one).
async fn new_requesters_since(
    control: &PgPool,
    account_id: Uuid,
    since: Option<DateTime<Utc>>,
) -> Result<Vec<NewRequester>, String> {
    let mut tx = begin_tenant_system_scoped(control, account_id)
        .await
        .map_err(|e| format!("org_join_request_digest: failed to open scoped read transaction: {e}"))?;
    let rows: Vec<(Uuid, Option<String>)> = sqlx::query_as(
        "SELECT r.user_id, u.name \
         FROM org_join_requests r \
         LEFT JOIN users u ON u.id = r.user_id \
         WHERE r.account_id = $1 AND r.decision IS NULL \
           AND r.requested_at > COALESCE($2, '-infinity'::timestamptz) \
         ORDER BY r.requested_at ASC",
    )
    .bind(account_id)
    .bind(since)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| format!("org_join_request_digest: new-requester query failed: {e}"))?;
    tx.rollback().await.ok();
    Ok(rows.into_iter().map(|(user_id, name)| NewRequester { user_id, name }).collect())
}

/// This admin's own most recent digest `sent_at` for `account_id`, across
/// every prior day — `None` if they have never been sent one.
async fn last_digest_sent_at(
    control: &PgPool,
    account_id: Uuid,
    admin_user_id: Uuid,
) -> Result<Option<DateTime<Utc>>, String> {
    let mut tx = begin_tenant_system_scoped(control, account_id)
        .await
        .map_err(|e| format!("org_join_request_digest: failed to open scoped read transaction: {e}"))?;
    let last: Option<DateTime<Utc>> = sqlx::query_scalar(
        "SELECT MAX(sent_at) FROM org_join_request_digests \
          WHERE account_id = $1 AND admin_user_id = $2",
    )
    .bind(account_id)
    .bind(admin_user_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| format!("org_join_request_digest: last-sent-at query failed: {e}"))?;
    tx.rollback().await.ok();
    Ok(last)
}

/// Already sent a digest to this admin for `account_id` TODAY — the
/// idempotency read half of the PK; [`process_account`]'s own claim INSERT
/// is the write half (`ON CONFLICT ... DO NOTHING`), which is what actually
/// makes a race between two concurrent ticks safe. This SELECT is a cheap
/// early skip so a same-day re-tick does not even reach the `new_since`
/// query for an admin it is definitely not about to email.
async fn already_sent_today(
    control: &PgPool,
    account_id: Uuid,
    admin_user_id: Uuid,
    sent_on: NaiveDate,
) -> Result<bool, String> {
    let mut tx = begin_tenant_system_scoped(control, account_id)
        .await
        .map_err(|e| format!("org_join_request_digest: failed to open scoped read transaction: {e}"))?;
    let already: bool = sqlx::query_scalar(
        "SELECT EXISTS ( \
             SELECT 1 FROM org_join_request_digests \
              WHERE account_id = $1 AND admin_user_id = $2 AND sent_on = $3 \
         )",
    )
    .bind(account_id)
    .bind(admin_user_id)
    .bind(sent_on)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| format!("org_join_request_digest: already-sent-today check failed: {e}"))?;
    tx.rollback().await.ok();
    Ok(already)
}

/// Per-tick outcome — the "alertable skip counters"
/// `scripts::check_silent_drop_is_alertable.sh` asks for in spirit (this
/// worker's own drops are logged, not silent) and this module's own pg
/// suite reads directly, same `pub(crate)` posture as `member_connect_
/// nudge::Outcome`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Outcome {
    pub(crate) digests_sent: usize,
    pub(crate) digests_skipped_already_today: usize,
    pub(crate) digests_skipped_no_new: usize,
    pub(crate) send_failed: usize,
    pub(crate) admins_without_address: usize,
}

/// `{n} person`/`{n} people asked to join {org} on CoralSwarm` — D13's own
/// example sentence, verbatim for `n = 3`.
///
/// # Two sections, three subjects (CS-447, D13 as amended 2026-09-22)
///
/// ```text
/// asked only   -> "3 people asked to join Acme on CoralSwarm"   (D13, unchanged)
/// added only   -> "2 people were added to Acme on CoralSwarm"  (1 -> "1 person was added")
/// both         -> "3 people asked to join Acme, 2 were added automatically"
/// ```
///
/// The asked-only line is byte-identical to what D13 specified and what
/// shipped in CS-438 — an org with auto-add off (every org, by default)
/// sees no change at all. The added-only line says **"were added"**, past
/// tense, because that is what happened: nobody is being asked to do
/// anything. And the combined line names BOTH counts rather than hiding
/// the automatic one in the body, because the automatic one is the one
/// that spent money without asking.
fn subject_line(request_count: usize, auto_add_count: usize, org_label: &str) -> String {
    let people = |n: usize| if n == 1 { "person" } else { "people" };
    match (request_count, auto_add_count) {
        (0, 0) => {
            // Unreachable: `process_account` never claims a digest with
            // nothing in it, and migration 0247's own
            // `request_count + auto_add_count > 0` refuses the row. Kept
            // total rather than `unreachable!()` — a subject line is not
            // worth a panic in a worker holding an open transaction.
            format!("Join activity for {org_label} on CoralSwarm")
        }
        (r, 0) => format!("{r} {} asked to join {org_label} on CoralSwarm", people(r)),
        (0, a) => format!(
            "{a} {} {} added to {org_label} on CoralSwarm",
            people(a),
            if a == 1 { "was" } else { "were" }
        ),
        (r, a) => format!(
            "{r} {} asked to join {org_label}, {a} {} added automatically",
            people(r),
            if a == 1 { "was" } else { "were" }
        ),
    }
}

/// How a rung reads to an admin who has never heard the word "rung".
///
/// D13's amendment wants the admin to know *which rung they qualified on*;
/// `"member_and_domain"` is a database value, not an English sentence. The
/// two strings below are `services::assertion_ladder::Rung::label`'s own,
/// and `org_auto_adds`' `CHECK` admits only those two — an unrecognised
/// value degrades to a neutral phrase rather than being rendered raw into
/// an email.
fn rung_label(rung: &str) -> &'static str {
    match rung {
        "directory" => "listed in your directory",
        "member_and_domain" => "full member of your workspace, on your verified domain",
        _ => "matched your auto-add rules",
    }
}

const FONT_SANS: &str = "Geist,-apple-system,'Segoe UI',system-ui,sans-serif";
const FONT_SERIF: &str = "'Instrument Serif',Georgia,'Times New Roman',serif";
const FONT_MONO: &str = "'JetBrains Mono',ui-monospace,Consolas,monospace";

/// Minimal HTML entity escaping — own copy, same "own copy matrix, no
/// coupling" reasoning `services::unmatched_recipient_email::escape_html`'s
/// own doc comment gives for why every email-rendering module in this crate
/// keeps its own rather than sharing one.
fn escape_html(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(c),
        }
    }
    out
}

/// A requester's display label for the email body — their name if Clerk/the
/// signup flow ever captured one, else "someone" (never their email —
/// D13's brief: "the count, the requesters' names (no emails in the body —
/// the admin sees those in-app)"). Trimmed; an all-whitespace stored name
/// degrades to "someone" the same as a missing one.
fn requester_label(name: Option<&str>) -> String {
    match name.map(str::trim) {
        Some(n) if !n.is_empty() => n.to_string(),
        _ => "someone".to_string(),
    }
}

/// The target org's display name for the subject/body, via the same
/// `ClerkAdminClient::get_organization` lookup `routes::org_join_requests::
/// list_mine` already makes for its own per-request org name. Best-effort:
/// no `ClerkAdminClient` configured, or the lookup fails, degrades to the
/// generic "your organization" — the SAME fallback `org_invites::
/// invite_copy` uses when it has no org label to work with — rather than
/// blocking the digest over a display-name nicety.
async fn org_display_label(clerk_org_id: &str) -> String {
    let Some(clerk) = ClerkAdminClient::from_env() else {
        return "your organization".to_string();
    };
    match clerk.get_organization(clerk_org_id).await {
        Ok(org) => {
            let trimmed = org.name.trim();
            if trimmed.is_empty() {
                "your organization".to_string()
            } else {
                trimmed.chars().take(80).collect()
            }
        }
        Err(_) => "your organization".to_string(),
    }
}

/// Render the digest email body — same cream-document shell as
/// `services::unmatched_recipient_email::render_connect_nudge_email_html`/
/// `routes::org_invites::render_invite_email_html` (their own doc comments
/// give the email-client-constraint reasoning for every choice here: table
/// layout, inline styles, no webfont loads, Georgia/system-sans/Consolas
/// fallback stacks). Voice per the brand skill: the serif heading carries
/// the human fact (who asked), sans explains, mono is provenance (the
/// count, "today").
fn render_digest_email_html(
    org_label: &str,
    names: &[String],
    auto_adds: &[AutoAddReport],
    members_url: &str,
) -> String {
    let count = names.len();
    let auto_count = auto_adds.len();
    let word = if count == 1 { "person" } else { "people" };
    let auto_word = if auto_count == 1 { "person" } else { "people" };
    let org_esc = escape_html(org_label);
    let url_esc = escape_html(members_url);
    // The heading leads with whichever section this digest is actually
    // about. When BOTH are present, the decision leads — an admin's
    // attention belongs on the thing that is waiting for them, and the
    // auto-add section below states its own fact in its own voice.
    let heading = if count > 0 {
        format!(
            "{count} {word} asked to join <em style=\"font-style:italic;color:#D14A38\">{org_esc}</em>"
        )
    } else {
        format!(
            "{auto_count} {auto_word} joined <em style=\"font-style:italic;color:#D14A38\">{org_esc}</em>"
        )
    };
    let name_rows: String = names
        .iter()
        .map(|n| {
            format!(
                "<li style=\"margin:0 0 4px 0;font-size:14px;line-height:1.5;color:#4A3F36\">{}</li>",
                escape_html(n)
            )
        })
        .collect();

    // ── Section 1: people ASKING. Omitted entirely when nobody asked —
    //    an empty "no one asked" block is the kind of thing D13's own
    //    "an empty digest trains people to ignore the full one" warns
    //    about, one level down.
    let ask_section = if count == 0 {
        String::new()
    } else {
        format!(
            "<p style=\"margin:0 0 16px 0;font-size:14px;line-height:1.6;color:#4A3F36\">They asked to join — no one is in yet until you approve them.</p>\
<ul style=\"margin:0 0 22px 0;padding-left:18px\">{name_rows}</ul>\
<table role=\"presentation\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\" style=\"margin-bottom:12px\"><tr>\
<td bgcolor=\"#F77764\" style=\"border-radius:8px\">\
<a href=\"{url_esc}\" style=\"display:inline-block;padding:11px 22px;font-family:{FONT_SANS};font-size:15px;font-weight:500;color:#FFFFFF;text-decoration:none\">Review requests</a>\
</td></tr></table>"
        )
    };

    // ── Section 2: people ADDED. D13's 2026-09-22 amendment. Visually
    //    distinct from section 1 by construction, not by hope: its own
    //    rule, its own mono eyebrow, a muted panel rather than the cream
    //    body, no call to action, and the rung on every row. One asks for
    //    a decision; this reports a fact that is already true.
    let auto_section = if auto_count == 0 {
        String::new()
    } else {
        let rows: String = auto_adds
            .iter()
            .map(|a| {
                format!(
                    "<li style=\"margin:0 0 6px 0;font-size:14px;line-height:1.5;color:#4A3F36\">{}<span style=\"font-family:{FONT_MONO};font-size:10px;color:#8A7B6E\"> · {}</span></li>",
                    escape_html(&requester_label(a.name.as_deref())),
                    escape_html(rung_label(&a.rung))
                )
            })
            .collect();
        format!(
            "<table role=\"presentation\" width=\"100%\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\" style=\"margin:8px 0 4px 0\"><tr>\
<td bgcolor=\"#F2EBE0\" style=\"border:1px solid #D9CCB8;border-radius:10px;padding:18px 20px\">\
<div style=\"font-family:{FONT_MONO};font-size:10px;text-transform:uppercase;letter-spacing:0.14em;color:#8A7B6E;margin-bottom:10px\">added automatically</div>\
<p style=\"margin:0 0 12px 0;font-size:14px;line-height:1.6;color:#4A3F36\">{auto_count} {auto_word} matched your auto-add rules and {} already in — each one used a slot you had already bought.</p>\
<ul style=\"margin:0;padding-left:18px\">{rows}</ul>\
</td></tr></table>",
            if auto_count == 1 { "is" } else { "are" }
        )
    };

    // Escaped like every other interpolation here: `subject_line` builds a
    // sentence from an org label that came from Clerk.
    let preheader = escape_html(&subject_line(count, auto_count, org_label));

    format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width\"></head>\
<body style=\"margin:0;padding:0;background:#F2EBE0\">\
<div style=\"display:none;max-height:0;overflow:hidden;mso-hide:all\">{preheader}</div>\
<table role=\"presentation\" width=\"100%\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\" bgcolor=\"#F2EBE0\"><tr><td align=\"center\" style=\"padding:40px 16px\">\
<table role=\"presentation\" width=\"560\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\" style=\"width:560px;max-width:100%\">\
<tr><td bgcolor=\"#FBF7F2\" style=\"border:1px solid #D9CCB8;border-radius:12px;padding:34px 36px;font-family:{FONT_SANS};color:#1A1410\">\
<div style=\"margin-bottom:26px;font-weight:600;font-size:19px;letter-spacing:-0.045em;color:#1A1410;line-height:1\">coralswarm</div>\
<div style=\"font-family:{FONT_MONO};font-size:10px;text-transform:uppercase;letter-spacing:0.14em;color:#8A7B6E;margin-bottom:12px\">join activity</div>\
<h1 style=\"margin:0 0 14px 0;font-family:{FONT_SERIF};font-size:30px;font-weight:400;line-height:1.2;color:#1A1410\">{heading}</h1>\
{ask_section}\
{auto_section}\
<table role=\"presentation\" width=\"100%\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\" style=\"margin-top:14px\"><tr>\
<td style=\"border-top:1px solid #D9CCB8;padding-top:16px\">\
<p style=\"margin:0;font-family:{FONT_MONO};font-size:9.5px;line-height:1.7;color:#8A7B6E\">You're getting this because you're an admin of {org_esc} on coralswarm. At most one of these a day, only when someone newly asks to join or is added automatically.</p>\
</td></tr></table>\
</td></tr>\
<tr><td align=\"center\" style=\"padding-top:14px\">\
<p style=\"margin:0;font-family:{FONT_MONO};font-size:9.5px;color:#8A7B6E\">coralswarm · your team's living memory</p>\
</td></tr></table>\
</td></tr></table></body></html>"
    )
}

fn resend_base_url() -> String {
    std::env::var("RESEND_API_BASE_URL").unwrap_or_else(|_| "https://api.resend.com".to_string())
}

/// The domain half of an email address, for logging — never the full
/// address, same PII discipline every sender in this crate follows.
fn email_domain(email: &str) -> &str {
    email.split('@').nth(1).unwrap_or(email)
}

/// Send one digest email via Resend. Best-effort: any failure (missing key,
/// non-2xx, transport error, or a hang past 10s) returns `false` and never
/// panics — the caller ([`process_account`]) holds the claim transaction
/// open across this call and rolls it back on `false` (module doc's "Claim
/// vs send order" section), so a failed send here costs a retry on the next
/// tick, never a lost or duplicated digest.
async fn send_digest_email(
    state: &AppState,
    to_email: &str,
    org_label: &str,
    names: &[String],
    auto_adds: &[AutoAddReport],
    members_url: &str,
) -> bool {
    let Some(resend_key) = state.resend_api_key.clone() else {
        return false;
    };
    let payload = serde_json::json!({
        "from": "coralswarm <person@example.com>",
        "to": [to_email],
        "subject": subject_line(names.len(), auto_adds.len(), org_label),
        "html": render_digest_email_html(org_label, names, auto_adds, members_url),
    });
    let url = format!("{}/emails", resend_base_url());
    let send = state.http_client.post(&url).bearer_auth(&resend_key).json(&payload).send();
    match tokio::time::timeout(std::time::Duration::from_secs(10), send).await {
        Ok(Ok(resp)) if resp.status().is_success() => true,
        Ok(Ok(resp)) => {
            tracing::warn!(
                status = %resp.status(), email_domain = email_domain(to_email),
                "org_join_request_digest: Resend returned non-success for digest email"
            );
            false
        }
        Ok(Err(e)) => {
            tracing::warn!(
                error = %e, email_domain = email_domain(to_email),
                "org_join_request_digest: failed to send digest email via Resend"
            );
            false
        }
        Err(_) => {
            tracing::warn!(
                email_domain = email_domain(to_email),
                "org_join_request_digest: Resend send timed out"
            );
            false
        }
    }
}

/// Process every admin candidate for one account. `pub(crate)` so the pg
/// test suite can drive it directly with precise counts, same convention as
/// `member_connect_nudge::process_account`.
pub(crate) async fn process_account(
    control: &PgPool,
    state: &AppState,
    account_id: Uuid,
    clerk_org_id: &str,
) -> Result<Outcome, String> {
    let mut outcome = Outcome::default();

    // CS-447: BOTH halves. An org with an empty queue can still owe its
    // admins a digest because somebody was auto-added — see
    // `has_unreported_auto_adds`. `||`, short-circuiting on the cheaper,
    // pre-existing check first.
    if !has_open_requests(control, account_id).await? && !has_unreported_auto_adds(control, account_id).await? {
        return Ok(outcome);
    }

    let admins = candidate_admins(control, account_id).await?;
    if admins.is_empty() {
        return Ok(outcome);
    }

    // Resolved once per account, not once per admin — same discipline
    // `member_connect_nudge::process_account`'s own `web_base` resolution
    // documents for itself.
    let web_base = state.org_web_base_url(control, clerk_org_id).await;
    let members_url = format!("{web_base}/settings/organization/members");
    let org_label = org_display_label(clerk_org_id).await;
    let sent_on = today();

    for admin in admins {
        if admin.email.trim().is_empty() {
            // Defensive only — `users.email` is `NOT NULL` (migration
            // `0001`) — kept rather than assumed, same posture `member_
            // connect_nudge::process_account`'s identical check documents.
            outcome.admins_without_address += 1;
            continue;
        }

        if already_sent_today(control, account_id, admin.admin_user_id, sent_on).await? {
            outcome.digests_skipped_already_today += 1;
            continue;
        }

        let since = last_digest_sent_at(control, account_id, admin.admin_user_id).await?;
        let new_requesters = new_requesters_since(control, account_id, since).await?;
        // Unlocked preview only — a miss here skips without writing sent_at,
        // so a concurrent in-flight auto-add is still visible next tick.
        let preview_auto_adds = new_auto_adds_since(control, account_id, since).await?;
        if new_requesters.is_empty() && preview_auto_adds.is_empty() {
            outcome.digests_skipped_no_new += 1;
            continue;
        }

        let mut tx = match begin_tenant_system_scoped(control, account_id).await {
            Ok(tx) => tx,
            Err(e) => {
                tracing::warn!(
                    error = %e, account_id = %account_id, admin_user_id = %admin.admin_user_id,
                    "org_join_request_digest: failed to open scoped transaction for the claim \
                     — skipping this admin, continuing with the rest"
                );
                continue;
            }
        };

        // Same lock `record_auto_add` takes. Held until this claim commits
        // or rolls back, so an in-flight INSERT either lands in THIS
        // select or waits and stamps `added_at` after our `sent_at`.
        if let Err(e) = auto_add::lock_auto_add_digest(&mut tx, account_id).await {
            tx.rollback().await.ok();
            tracing::warn!(
                error = %e, account_id = %account_id, admin_user_id = %admin.admin_user_id,
                "org_join_request_digest: failed to take the auto-add digest lock \
                 — skipping this admin, continuing with the rest"
            );
            continue;
        }
        let new_auto_adds = match new_auto_adds_since_on(&mut tx, account_id, since).await {
            Ok(rows) => rows,
            Err(e) => {
                tx.rollback().await.ok();
                tracing::warn!(
                    error = %e, account_id = %account_id, admin_user_id = %admin.admin_user_id,
                    "org_join_request_digest: locked auto-add re-read failed — skipping this \
                     admin, continuing with the rest"
                );
                continue;
            }
        };
        if new_requesters.is_empty() && new_auto_adds.is_empty() {
            tx.rollback().await.ok();
            outcome.digests_skipped_no_new += 1;
            continue;
        }
        let names: Vec<String> =
            new_requesters.iter().map(|r| requester_label(r.name.as_deref())).collect();
        let request_count = names.len() as i32;
        let auto_add_count = new_auto_adds.len() as i32;

        // Claim: the SAME `INSERT ... ON CONFLICT DO NOTHING` idempotency
        // shape `org_join_requests::enqueue_join_request` uses for its own
        // partial-unique-index race — this table's PRIMARY KEY plays the
        // identical role. A `None` here means a concurrent tick (another
        // process, or a legitimate race against this same worker's next
        // scheduled run) already claimed `(account_id, admin_user_id,
        // sent_on)` between this loop's own `already_sent_today` read above
        // and now — belt AND suspenders, not either/or, same discipline
        // `member_connect_nudge`'s own cooldown claim documents for itself.
        let claimed: Option<i32> = match sqlx::query_scalar(
            "INSERT INTO org_join_request_digests \
                 (account_id, admin_user_id, sent_on, request_count, auto_add_count) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (account_id, admin_user_id, sent_on) DO NOTHING \
             RETURNING request_count",
        )
        .bind(account_id)
        .bind(admin.admin_user_id)
        .bind(sent_on)
        .bind(request_count)
        // CS-447 (migration 0247). `request_count` may now legitimately be
        // 0 — a digest sent purely because people were auto-added — and
        // 0247's own `request_count + auto_add_count > 0` is what still
        // refuses a genuinely empty one.
        .bind(auto_add_count)
        .fetch_optional(&mut *tx)
        .await
        {
            Ok(c) => c,
            Err(e) => {
                tx.rollback().await.ok();
                tracing::warn!(
                    error = %e, account_id = %account_id, admin_user_id = %admin.admin_user_id,
                    "org_join_request_digest: claim insert failed — skipping this admin, \
                     continuing with the rest"
                );
                continue;
            }
        };
        if claimed.is_none() {
            tx.rollback().await.ok();
            outcome.digests_skipped_already_today += 1;
            continue;
        }

        // Send WHILE the claim is still uncommitted (module doc's "Claim vs
        // send order" section) — commit only on success, roll back on
        // failure so a failed send never burns this admin's "sent today"
        // for a digest that never arrived.
        if send_digest_email(state, &admin.email, &org_label, &names, &new_auto_adds, &members_url).await {
            if let Err(e) = tx.commit().await {
                tracing::warn!(
                    error = %e, account_id = %account_id, admin_user_id = %admin.admin_user_id,
                    "org_join_request_digest: sent the digest but failed to commit its claim — \
                     a second digest may arrive today"
                );
            }
            outcome.digests_sent += 1;
        } else {
            tx.rollback().await.ok();
            outcome.send_failed += 1;
        }
    }

    Ok(outcome)
}

/// One pass for one [`WorkerTenant`] — see the module doc's "Shape" section
/// for why this is always control-plane work regardless of which tenant the
/// harness handed this tick, same `TenantId::Consumer` no-op precedent
/// `member_connect_nudge::tick` documents (no ordinary org ever lives on
/// the shared personal-account plane).
pub async fn tick(t: &WorkerTenant, control: &PgPool, state: &Arc<AppState>) -> Result<Tick, String> {
    let clerk_org_id = match &t.tenant {
        TenantId::Consumer => return Ok(Tick::idle()),
        TenantId::Org(id) => id.clone(),
    };

    let account_id = resolve_org_account_id(control, &clerk_org_id).await?;
    let outcome = process_account(control, state, account_id, &clerk_org_id).await?;

    tracing::info!(
        account_id = %account_id,
        clerk_org_id = %clerk_org_id,
        digests_sent = outcome.digests_sent,
        digests_skipped_already_today = outcome.digests_skipped_already_today,
        digests_skipped_no_new = outcome.digests_skipped_no_new,
        send_failed = outcome.send_failed,
        admins_without_address = outcome.admins_without_address,
        "org_join_request_digest: tick outcome"
    );

    let processed = outcome.digests_sent
        + outcome.digests_skipped_already_today
        + outcome.digests_skipped_no_new
        + outcome.send_failed
        + outcome.admins_without_address;
    Ok(Tick::worked(processed))
}

#[cfg(test)]
mod email_copy_tests {
    use super::*;

    #[test]
    fn auto_add_only_digest_uses_join_activity_shell() {
        let html = render_digest_email_html(
            "Acme",
            &[],
            &[AutoAddReport { name: Some("Gita".into()), rung: "directory".into() }],
            "https://example.test/members",
        );
        assert!(html.contains(">join activity<"), "eyebrow must cover auto-adds, not only requests");
        assert!(!html.contains(">join requests<"), "request-only eyebrow leaks into auto-add-only mail");
        assert!(
            html.contains("only when someone newly asks to join or is added automatically"),
            "footer must name auto-add; html was: {html}"
        );
        assert!(
            !html.contains("only when someone new has asked"),
            "the old request-only footer must not ship"
        );
    }

    #[test]
    fn request_only_digest_uses_the_same_neutral_shell() {
        let html = render_digest_email_html("Acme", &["Ines".into()], &[], "https://example.test/members");
        assert!(html.contains(">join activity<"));
        assert!(html.contains("only when someone newly asks to join or is added automatically"));
    }
}
