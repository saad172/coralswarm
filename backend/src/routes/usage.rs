//! `GET /v1/usage/summary` — usage-metering PR12 of 13
//! (`.scratch/usage-metering/plan.md` §1 PR12) — the endpoint behind the
//! organization Usage page, the organization Billing page, and the new
//! personal Usage page. All three used to hardcode the same fictional `$186`
//! (a mock value baked in before metering existed); this is the first real
//! read against the ledger PR8 defined and PR9 populates.
//!
//! ## Raw numbers, not pre-formatted strings
//! Every number below is exactly what it says it is (credits, USD, token
//! counts, row counts) — no percentages, no currency-formatted strings, no
//! hue/label decisions. The admin contract in this repo (`docs/design/
//! DESIGN_SYSTEM.md` + the admin app's own 5-step chain) is explicit that the
//! server sends data and the client decides presentation; the pre-existing
//! mock this replaces violated that (`pct: 84`, `value: "$182 · 289 asks"`
//! pre-baked as a display string) and is exactly what this endpoint must not
//! reproduce.
//!
//! ## Two pools, one account
//! `credit_ledger`/`credit_balances`/`plans`/`grants` are CONTROL-ONLY tables
//! (migration `0150_credit_ledger.sql`) — read via [`AppState::control_db`],
//! which is `state.db` **always**, regardless of which plane this request is
//! bound to (see that accessor's own doc comment). `llm_usage`/`query_history`/
//! `graphs` are CONTENT-plane tables that exist on both trees — read via the
//! [`Tenant`] extractor's `pool` (the org tenant pool under
//! `TenancyDecision::Org`, the same control pool under `Personal`). Both reads
//! use `user.account_id` (`Extension<UserContext>`), which is the SAME value
//! `TenantContext::account_id` carries for an org-bound request (both are
//! resolved from the same `resolve_clerk_context` call in
//! `middleware::auth::require_auth`), so there is exactly one billing account
//! per request regardless of which pool answers which part of this query.
//!
//! ## Org visibility: system-scoped, not member-scoped — and admin-only
//! The org branch is meant to show the WHOLE org's spend, not just the
//! oceans the calling admin happens to be a member of — so it opens
//! [`begin_tenant_system_scoped`], exactly like
//! `workers::billing_derivation`'s own Org branch, rather than a
//! member-scoped transaction. The personal branch opens
//! [`begin_account_scoped`], the ordinary consumer-plane RLS scope.
//!
//! Because that read is org-wide (the org's dollar spend, per-workspace
//! cost, and `top_askers` with every asker's email), an org-bound caller
//! must be an org admin: the handler runs
//! [`crate::routes::org_invites::require_org_admin`] — the same gate the
//! invite, join-request, slot-purchase and auto-add routes use — BEFORE any
//! read, and a plain member gets `403 {"error": "admin only"}`. The web's
//! `OrgGate` hiding the org Usage/Billing pages is presentation only; it was
//! the ONLY thing standing between a member and these numbers until settings
//! launch PR 2.6. A member is not given a trimmed member-scoped variant of
//! this document: no member-facing web surface reads it (the personal
//! Usage page only fetches on the personal plane), and the member's own
//! "Your usage" card is a separate, credits-only endpoint (settings launch
//! plan 2.7 / 3.6). Personal orgs never reach this branch: the gate resolves
//! them to `TenancyDecision::Personal` (`TokenKind::PersonalOrg`).
//!
//! ## Unattributed workspace bucket
//! `llm_usage.graph_id` is nullable, carries no FK, and several call sites
//! pass `None` by design (plan §3 item 19; `services::atom_title`
//! unconditionally, a handful of others conditionally). Grouping by
//! `graph_id` and mapping `NULL` to an explicit `unattributed: true` bucket —
//! rather than filtering `WHERE graph_id IS NOT NULL`, which would silently
//! drop that spend from the total — is required by the plan for exactly this
//! reason: a usage page that quietly drops rows under-reports, which is the
//! same class of bug this whole PR exists to fix.
//!
//! ## Cost estimate, not the ledger's own number
//! The by-workspace/by-asker breakdowns price `llm_usage` rows directly
//! against [`PriceSheet`], the SAME read-only, no-I/O pricing function
//! `workers::billing_derivation` uses — but this is deliberately NOT a read of
//! `credit_ledger` itself: `credit_ledger`'s job/query-axis debit rows don't
//! carry `graph_id` at all (only journal-axis rows do — see
//! `workers::billing_derivation::insert_ledger_row`), so there is no
//! per-workspace breakdown available from the ledger alone. The headline
//! balance/spend-this-period numbers ARE read straight from `credit_ledger`/
//! `credit_balances` (the authoritative, already-charged totals); the
//! workspace/asker breakdowns are a best-effort estimate over the same raw
//! `llm_usage` rows the ledger derives from, so they can disagree slightly
//! with the headline total (in particular: batch-path rows and cache-hit
//! markers appear in `llm_usage` but are not all separately ledgered yet —
//! see `workers::billing_derivation`'s own "classifying source-id-less rows"
//! section). `llm_usage.cost_usd` is never populated in this tree today (no
//! cost-reporting provider is wired — plan §2 item 7), so unlike the
//! derivation worker this handler does not bother branching on it; every
//! bucket here is priced from tokens via [`PriceSheet::rate`].
//!
//! ## Two epochs on one screen — why [`ledger_coverage_start`] exists
//! The headline tiles read `credit_ledger`, which began at deploy with no
//! backfill (plan §1 PR2: no pre-PR2 `llm_usage` row carries a source id to
//! join on), while `by_workspace`/`top_askers` read `llm_usage`/
//! `query_history`, which go back to the beginning of the content plane.
//! Both are correctly windowed to the same `period_start`, but the ledger
//! only *covers* part of that window in the first month metering runs — so a
//! real `spend: 0` sits next to a real `by_workspace: $155.79` and the screen
//! contradicts itself. Neither number is wrong; the missing fact is that the
//! ledger doesn't reach back to `period_start` yet.
//!
//! [`UsageSummaryResponse::ledger_coverage_start`] publishes that fact — the
//! account's oldest ledger row — so the client can say "nothing billed, the
//! ledger begins on the 13th" instead of "$0.00", which reads as "nothing
//! happened". It is a coverage floor, not a copy string: the server states
//! when billing data starts and the client decides how to phrase it, same
//! division of labour as every other field here.
//!
//! ## Never fabricate on error
//! Every DB read below either produces a real (possibly zero) number or this
//! handler returns 500 — mirroring `routes::admin`'s `fail_loud!` discipline.
//! A zero balance/zero spend from a REAL empty read is an honest answer (the
//! negative control this PR ships against); a swallowed error defaulting to
//! the same zero would not be, so read failures never fall through to a
//! default value here.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Extension, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use chrono::{DateTime, Datelike, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use crate::billing::rates::PriceSheet;
use crate::middleware::auth::UserContext;
use crate::tenancy::extractor::Tenant;
use crate::tenancy::runtime::TenancyDecision;
use crate::tenancy::scope::{begin_account_scoped, begin_tenant_system_scoped};
use crate::AppState;

// ── Wire types ───────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
pub struct UsageSummaryResponse {
    /// Start of the reporting period (current UTC calendar month), inclusive.
    pub period_start: DateTime<Utc>,
    /// End of the reporting period — always "now" (this is a to-date report,
    /// never a closed historical month).
    pub period_end: DateTime<Utc>,
    /// `credit_balances.balance` — the materialized current balance. `0.0`
    /// when no row exists yet (an account with no metered activity and no
    /// grant), which is a real, honest zero, not a placeholder.
    pub balance_credits: f64,
    /// The account's active (unrevoked) plan/grant, if any. `None` is
    /// expected and honest today: PR10 (plan seeding) has not shipped in
    /// this tree yet, so `grants` is empty for every account.
    pub plan: Option<PlanSummary>,
    /// USD one credit represents, from `credit_conversion` — `None` only if
    /// the table has no row active as of the period start (a seeding bug,
    /// not a runtime condition; see `billing::credit_conversion`'s own doc).
    pub credit_conversion_usd_per_credit: Option<f64>,
    pub spend: SpendSummary,
    /// By ocean (`llm_usage.graph_id`), for this period. Always includes an
    /// `unattributed: true` entry when any row this period carried no
    /// `graph_id` — see the module doc's "Unattributed workspace bucket".
    pub by_workspace: Vec<WorkspaceSpend>,
    /// Busiest askers this period, from `query_history`, most asks first.
    pub top_askers: Vec<AskerStat>,
    /// `COUNT(atoms)` ingested this period (by `created_at`, not the source
    /// event's own `timestamp`), scoped exactly like [`Self::by_workspace`]:
    /// this account's graphs when personal, every graph this system-scoped
    /// transaction can see when org.
    pub atoms_ingested: i64,
    /// Whether `credit_ledger` has ever had a row for this account, at all
    /// (not scoped to this period) — lets the client tell "genuinely never
    /// metered yet" apart from "metered, just quiet this month" without
    /// guessing from a zero.
    pub has_ledger_history: bool,
    /// `created_at` of the OLDEST `credit_ledger` row for this account, ever
    /// (not scoped to this period); `None` exactly when `has_ledger_history`
    /// is `false`. Nothing before this instant was ever ledgered, so it is
    /// the account's billing-coverage floor: when it lands AFTER
    /// `period_start`, a zero `spend` means "not billed yet", not "nothing
    /// happened" — see the module doc's "Two epochs on one screen".
    pub ledger_coverage_start: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PlanSummary {
    pub code: String,
    pub name: String,
    /// This grant's TOTAL credit allowance —
    /// `COALESCE(grants.credits_override, plans.credits * COALESCE(grants.seats, 1))`.
    ///
    /// Already multiplied by [`Self::seats`] for a seat-priced plan, so a
    /// client must NOT multiply again. See migration `0156_grants_seats.sql`,
    /// which states this formula once for both call sites (the other is
    /// `bin/grant_plan.rs`).
    pub credits: f64,
    /// Purchased seats for a seat-priced plan — `team_standard`,
    /// `team_premium`, or the mixed-tier `team` umbrella plan (0167) —
    /// `None` for a flat one (`free`, `pro`, `founder_comp`). The schema
    /// makes those the only two possibilities: `grants_seats_iff_seat_priced`
    /// (migration `0156`) rejects a seat-priced grant with no seat count and
    /// a flat grant that carries one — so `None` here means "flat plan",
    /// never "seat count unknown".
    ///
    /// **Stale-doc note (billing self-serve launch PR E, code review): this
    /// field's `Some`-ness no longer implies [`Self::credits_per_seat`] is
    /// also `Some`.** Before `0167`/`0169`, "seat-priced" meant exactly
    /// `team_standard`/`team_premium`, both single-price-point plans where a
    /// `Some(seats)` always paired with a `Some(credits_per_seat)`. `team` is
    /// a THIRD seat-priced plan, and unlike the other two it can mix
    /// `standard`/`premium` seats on one grant (plan §1.4) — so it carries a
    /// real `Some(seats)` (the total across every tier) but has no single
    /// per-seat figure to report, and [`Self::credits_per_seat`] is `None`
    /// for it unconditionally. See that field's own doc comment for the
    /// reasoning.
    pub seats: Option<i32>,
    /// The plan's PER-SEAT credit allowance — `plans.credits` unmultiplied
    /// — so a client can render "3,000 x 4 seats" without deriving it as
    /// `credits / seats`.
    ///
    /// `None` in three cases (a stale version of this comment said two —
    /// see [`Self::seats`]'s own doc comment for the post-0167/0169
    /// correction):
    ///   * a FLAT plan, where it would merely repeat [`Self::credits`];
    ///   * a grant carrying `credits_override`, where an absolute total
    ///     wins over the multiplication entirely. Publishing a per-seat
    ///     figure there would be CONTRADICTORY, not just redundant — a
    ///     client would render "4 seats x 3,000" beside a total of 7,000
    ///     and look broken. The API suppresses it rather than leaving every
    ///     client to rediscover the trap;
    ///   * a mixed-tier `team` grant (0167), unconditionally — it can hold
    ///     BOTH `standard` and `premium` seats at once, so there is no
    ///     single per-seat figure to report even without an override.
    ///
    /// So `credits_per_seat * seats == credits` holds WHENEVER this field
    /// is `Some`, and a client never has to ask whether an override is in
    /// play.
    pub credits_per_seat: Option<f64>,
    pub refresh_kind: String,
    pub granted_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SpendSummary {
    /// Credits debited this period, as a POSITIVE number (`credit_ledger.
    /// credits` is stored negative for a debit; this is `-credits`, summed).
    pub total_credits: f64,
    /// `credit_ledger.cost_usd` summed over the same rows, positive.
    pub total_cost_usd: f64,
    /// One entry per `credit_ledger.entry_kind` this period ("usage_job" /
    /// "usage_query" / "usage_journal" — `workers::billing_derivation`'s
    /// `ENTRY_KIND_*` constants). Empty when there is no ledger activity
    /// this period, never a fabricated placeholder row.
    pub by_axis: Vec<AxisSpend>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AxisSpend {
    pub axis: String,
    pub credits: f64,
    pub cost_usd: f64,
    pub count: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkspaceSpend {
    /// `None` exactly when `unattributed` is `true`.
    pub graph_id: Option<Uuid>,
    /// `graphs.name`, resolved on the same content-plane pool this row's
    /// `llm_usage` read came from. `None` if the graph has since been
    /// deleted (a real, if unusual, state — not a data error).
    pub name: Option<String>,
    /// Estimated USD for this workspace this period — see the module doc's
    /// "Cost estimate, not the ledger's own number".
    pub cost_usd: f64,
    pub unattributed: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AskerStat {
    /// `None` for system/worker-authored rows with no acting user — kept
    /// visible rather than dropped, same "don't silently absorb" posture as
    /// the workspace bucket above.
    pub user_id: Option<Uuid>,
    pub email: Option<String>,
    pub asks: i64,
}

// ── Handler ──────────────────────────────────────────────────────────────

pub async fn get_usage_summary(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
) -> Response {
    // Authorization first, before any read or availability check: an org
    // member must learn nothing from this endpoint, not even whether the
    // database is up. See the module doc's "Org visibility" section.
    let is_org = matches!(tenant.decision, Some(TenancyDecision::Org(_)));
    if is_org {
        if let Err(resp) = crate::routes::org_invites::require_org_admin(&tenant, &user) {
            tracing::info!(
                target: "usage.summary",
                user_id = %user.user_id,
                account_id = %user.account_id,
                role = %user.role,
                "refusing org usage summary to a non-admin org member"
            );
            return resp;
        }
    }
    let Some(control) = state.control_db() else {
        return service_unavailable();
    };
    let Some(content_pool) = tenant.pool.clone() else {
        return service_unavailable();
    };

    let now = Utc::now();
    let period_start = month_start(now);
    // Pricing anchor: the REPORTED period's start, never `now()` — see
    // `PriceSheet::rate`'s own doc comment and plan §4 item 5 ("using now()
    // silently reprices history"). This is a to-date report, so `now()` is
    // legitimately used above to define what "this period" even is; it must
    // never additionally be used to pick a rate.
    let pricing_as_of = period_start;

    let sheet = match PriceSheet::load(control).await {
        Ok(s) => s,
        Err(e) => return internal_error("usage.price_sheet", e),
    };
    let conversion_table = match crate::billing::credit_conversion::CreditConversionTable::load(control).await {
        Ok(t) => t,
        Err(e) => return internal_error("usage.credit_conversion", e),
    };
    let usd_per_credit: Option<f64> = conversion_table
        .active(pricing_as_of)
        .and_then(|row| row.reference_cost_per_credit.to_string().parse::<f64>().ok());

    // ── Control plane: balance, plan, ledger rows this period ────────────
    let mut ctrl_tx = match begin_account_scoped(control, user.account_id).await {
        Ok(tx) => tx,
        Err(e) => return internal_error("usage.control_scope", e),
    };

    let balance_credits: f64 = match sqlx::query_scalar::<_, f64>(
        "SELECT balance::float8 FROM credit_balances WHERE account_id = $1",
    )
    .bind(user.account_id)
    .fetch_optional(&mut *ctrl_tx)
    .await
    {
        // `fetch_optional`'s `None` is "no `credit_balances` row yet" (an
        // account with no metered activity at all) — a real, honest zero,
        // never a fabricated one; `balance` itself is NOT NULL, so there is
        // no second `Option` layer to collapse.
        Ok(row) => row.unwrap_or(0.0),
        Err(e) => return internal_error("usage.balance", e),
    };

    // The query itself lives in `billing::entitlement` so the tests that
    // claim to prove this formula run THIS code rather than a re-typed copy
    // of it — see that module's header for why the copy was a hole.
    let plan: Option<PlanSummary> =
        match crate::billing::entitlement::current_plan(&mut *ctrl_tx, user.account_id).await {
            Ok(row) => row.map(|r| PlanSummary {
                code: r.code,
                name: r.name,
                credits: r.credits,
                seats: r.seats,
                credits_per_seat: r.credits_per_seat,
                refresh_kind: r.refresh_kind,
                granted_at: r.granted_at,
            }),
            Err(e) => return internal_error("usage.plan", e),
        };

    // `MIN(created_at)`, not `EXISTS` — one read answers BOTH "has this
    // account ever been metered" and "from when is its spend actually
    // billed" (the module doc's "Two epochs on one screen"). Safe to collapse
    // the two: `credit_ledger.created_at` is `TIMESTAMPTZ NOT NULL`
    // (migration `0150_credit_ledger.sql`), so a NULL here is always "no rows
    // at all", never "a row with a missing timestamp". `MIN` over an empty
    // set still returns one row, hence `fetch_one` over an `Option` column
    // rather than `fetch_optional`. Served by `idx_credit_ledger_account_
    // created (account_id, created_at DESC)` — a DESC index answers `MIN`
    // with a backward scan, so this stays an index probe, not a table scan.
    let ledger_coverage_start: Option<DateTime<Utc>> =
        match sqlx::query_scalar::<_, Option<DateTime<Utc>>>(
            "SELECT MIN(created_at) FROM credit_ledger WHERE account_id = $1",
        )
        .bind(user.account_id)
        .fetch_one(&mut *ctrl_tx)
        .await
        {
            Ok(v) => v,
            Err(e) => return internal_error("usage.ledger_coverage_start", e),
        };
    let has_ledger_history: bool = ledger_coverage_start.is_some();

    #[derive(sqlx::FromRow)]
    struct LedgerRow {
        entry_kind: String,
        credits: f64,
        cost_usd: f64,
    }
    let ledger_rows: Vec<LedgerRow> = match sqlx::query_as::<_, LedgerRow>(
        // `-credits` flips the ledger's stored-negative debit into a
        // positive "amount spent" for display — see `SpendSummary::
        // total_credits`'s own doc comment. `credits < 0` excludes any
        // future grant row (PR10; positive credits) from a "spend" total.
        "SELECT entry_kind, (-credits)::float8 AS credits, COALESCE(cost_usd, 0)::float8 AS cost_usd \
         FROM credit_ledger \
         WHERE account_id = $1 AND created_at >= $2 AND credits < 0",
    )
    .bind(user.account_id)
    .bind(period_start)
    .fetch_all(&mut *ctrl_tx)
    .await
    {
        Ok(rows) => rows,
        Err(e) => return internal_error("usage.ledger_rows", e),
    };

    // Read-only transaction — nothing above wrote anything, but commit
    // rather than let it implicitly roll back on drop, matching this
    // codebase's other scoped-read call sites.
    if let Err(e) = ctrl_tx.commit().await {
        return internal_error("usage.control_commit", e);
    }

    let mut by_axis: HashMap<String, (f64, f64, i64)> = HashMap::new();
    let mut total_credits = 0.0f64;
    let mut total_cost_usd = 0.0f64;
    for row in &ledger_rows {
        total_credits += row.credits;
        total_cost_usd += row.cost_usd;
        let entry = by_axis.entry(row.entry_kind.clone()).or_insert((0.0, 0.0, 0));
        entry.0 += row.credits;
        entry.1 += row.cost_usd;
        entry.2 += 1;
    }
    let mut by_axis: Vec<AxisSpend> = by_axis
        .into_iter()
        .map(|(axis, (credits, cost_usd, count))| AxisSpend { axis, credits, cost_usd, count })
        .collect();
    by_axis.sort_by(|a, b| a.axis.cmp(&b.axis));

    // ── Content plane: llm_usage by workspace, query_history by asker ────
    // Org: system-scoped (whole tenant, not just the caller's own oceans —
    // see the module doc's "Org visibility" section). Personal: ordinary
    // account-scoped consumer RLS.
    let content_scope = if is_org {
        begin_tenant_system_scoped(&content_pool, user.account_id).await
    } else {
        begin_account_scoped(&content_pool, user.account_id).await
    };
    let mut content_tx = match content_scope {
        Ok(tx) => tx,
        Err(e) => return internal_error("usage.content_scope", e),
    };

    #[derive(sqlx::FromRow)]
    struct UsageBucketRow {
        graph_id: Option<Uuid>,
        model: String,
        input_tokens: i64,
        output_tokens: i64,
        cache_creation_tokens: i64,
        cache_read_tokens: i64,
    }
    // Org-plane rows frequently carry `account_id IS NULL` by design (worker
    // call sites — see `workers::billing_derivation`'s own "journal axis"
    // note), so the org branch reads everything this system-scoped
    // transaction can see rather than filtering by account_id; the Personal
    // branch filters explicitly, since `llm_usage` is shared across every
    // consumer account on that plane.
    let usage_buckets: Result<Vec<UsageBucketRow>, sqlx::Error> = if is_org {
        sqlx::query_as(
            "SELECT graph_id, model, \
                    COALESCE(SUM(input_tokens), 0)::bigint AS input_tokens, \
                    COALESCE(SUM(output_tokens), 0)::bigint AS output_tokens, \
                    COALESCE(SUM(cache_creation_input_tokens), 0)::bigint AS cache_creation_tokens, \
                    COALESCE(SUM(cache_read_input_tokens), 0)::bigint AS cache_read_tokens \
             FROM llm_usage \
             WHERE created_at >= $1 \
             GROUP BY graph_id, model",
        )
        .bind(period_start)
        .fetch_all(&mut *content_tx)
        .await
    } else {
        sqlx::query_as(
            "SELECT graph_id, model, \
                    COALESCE(SUM(input_tokens), 0)::bigint AS input_tokens, \
                    COALESCE(SUM(output_tokens), 0)::bigint AS output_tokens, \
                    COALESCE(SUM(cache_creation_input_tokens), 0)::bigint AS cache_creation_tokens, \
                    COALESCE(SUM(cache_read_input_tokens), 0)::bigint AS cache_read_tokens \
             FROM llm_usage \
             WHERE account_id = $1 AND created_at >= $2 \
             GROUP BY graph_id, model",
        )
        .bind(user.account_id)
        .bind(period_start)
        .fetch_all(&mut *content_tx)
        .await
    };
    let usage_buckets = match usage_buckets {
        Ok(rows) => rows,
        Err(e) => return internal_error("usage.workspace_buckets", e),
    };

    let mut by_graph: HashMap<Option<Uuid>, f64> = HashMap::new();
    for row in &usage_buckets {
        let cost = estimate_cost_usd(
            &sheet,
            pricing_as_of,
            &row.model,
            row.input_tokens,
            row.output_tokens,
            row.cache_creation_tokens,
            row.cache_read_tokens,
        );
        *by_graph.entry(row.graph_id).or_insert(0.0) += cost;
    }

    let known_graph_ids: Vec<Uuid> = by_graph.keys().filter_map(|g| *g).collect();
    let graph_names: HashMap<Uuid, String> = if known_graph_ids.is_empty() {
        HashMap::new()
    } else {
        match sqlx::query_as::<_, (Uuid, String)>("SELECT id, name FROM graphs WHERE id = ANY($1)")
            .bind(&known_graph_ids)
            .fetch_all(&mut *content_tx)
            .await
        {
            Ok(rows) => rows.into_iter().collect(),
            Err(e) => return internal_error("usage.graph_names", e),
        }
    };

    let mut by_workspace: Vec<WorkspaceSpend> = by_graph
        .into_iter()
        .map(|(graph_id, cost_usd)| WorkspaceSpend {
            graph_id,
            name: graph_id.and_then(|g| graph_names.get(&g).cloned()),
            cost_usd,
            unattributed: graph_id.is_none(),
        })
        .collect();
    by_workspace.sort_by(|a, b| b.cost_usd.partial_cmp(&a.cost_usd).unwrap_or(std::cmp::Ordering::Equal));

    // `query_history.account_id` is NOT NULL on both planes and is always
    // this same billing account (unlike `llm_usage`'s worker sites), so no
    // is_org branch is needed here — see the module doc's "Two pools, one
    // account" section.
    #[derive(sqlx::FromRow)]
    struct AskerRow {
        user_id: Option<Uuid>,
        asks: i64,
    }
    let asker_rows: Vec<AskerRow> = match sqlx::query_as(
        "SELECT user_id, COUNT(*) AS asks \
         FROM query_history \
         WHERE account_id = $1 AND created_at >= $2 \
         GROUP BY user_id \
         ORDER BY asks DESC \
         LIMIT 10",
    )
    .bind(user.account_id)
    .bind(period_start)
    .fetch_all(&mut *content_tx)
    .await
    {
        Ok(rows) => rows,
        Err(e) => return internal_error("usage.top_askers", e),
    };

    // `atoms` carries no `account_id` of its own — it is scoped only via
    // `graph_id` (FK to `graphs`, which does carry `account_id`) — so a
    // personal count joins through `graphs` explicitly. The org branch
    // mirrors `by_workspace`'s own "read everything this system-scoped
    // transaction can see" posture: RLS already restricts this transaction
    // to the tenant's own graphs, so no additional account_id filter is
    // needed or correct there. `created_at` (ingestion time) is used rather
    // than `timestamp` (the source event's original time) — a message from
    // last year captured today counts as ingested NOW, not backdated.
    let atoms_ingested: i64 = match if is_org {
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM atoms a JOIN graphs g ON g.id = a.graph_id \
             WHERE a.created_at >= $1",
        )
        .bind(period_start)
        .fetch_one(&mut *content_tx)
        .await
    } else {
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM atoms a JOIN graphs g ON g.id = a.graph_id \
             WHERE g.account_id = $1 AND a.created_at >= $2",
        )
        .bind(user.account_id)
        .bind(period_start)
        .fetch_one(&mut *content_tx)
        .await
    } {
        Ok(n) => n,
        Err(e) => return internal_error("usage.atoms_ingested", e),
    };

    if let Err(e) = content_tx.commit().await {
        return internal_error("usage.content_commit", e);
    }

    // Emails resolve from the CONTROL-plane `users` table regardless of
    // plane (org tenant DBs have no `users` table of their own — see
    // `tenancy::extractor::Tenant`'s module doc, "Control-plane note"), and
    // `users` carries no RLS policy (unlike every table read above), so this
    // is a plain unscoped read on `control`.
    let asker_user_ids: Vec<Uuid> = asker_rows.iter().filter_map(|r| r.user_id).collect();
    let asker_emails: HashMap<Uuid, String> = if asker_user_ids.is_empty() {
        HashMap::new()
    } else {
        match sqlx::query_as::<_, (Uuid, String)>("SELECT id, email FROM users WHERE id = ANY($1)")
            .bind(&asker_user_ids)
            .fetch_all(control)
            .await
        {
            Ok(rows) => rows.into_iter().collect(),
            Err(e) => return internal_error("usage.asker_emails", e),
        }
    };
    let top_askers: Vec<AskerStat> = asker_rows
        .into_iter()
        .map(|r| AskerStat {
            user_id: r.user_id,
            email: r.user_id.and_then(|u| asker_emails.get(&u).cloned()),
            asks: r.asks,
        })
        .collect();

    (
        StatusCode::OK,
        Json(UsageSummaryResponse {
            period_start,
            period_end: now,
            balance_credits,
            plan,
            credit_conversion_usd_per_credit: usd_per_credit,
            spend: SpendSummary { total_credits, total_cost_usd, by_axis },
            by_workspace,
            top_askers,
            atoms_ingested,
            has_ledger_history,
            ledger_coverage_start,
        }),
    )
        .into_response()
}

pub(crate) fn month_start(now: DateTime<Utc>) -> DateTime<Utc> {
    let naive = now
        .date_naive()
        .with_day(1)
        .expect("day 1 is always a valid day of any month");
    naive
        .and_hms_opt(0, 0, 0)
        .expect("midnight is always a valid time")
        .and_utc()
}

/// **OpenRouter provider port, PR1 — provider is hardcoded to `"anthropic"`
/// here.** Same deliberate, documented placeholder `routes::admin::
/// model_rate`/`workers::billing_derivation::estimate_cost_usd` use: the
/// `llm_usage` query feeding this handler `GROUP BY graph_id, model` only,
/// with no `provider` per bucket — rewiring it is a separate,
/// separately-reviewable reporting change. A no-op for every row this
/// endpoint has priced to date; fails safe (blend-rate estimate, not a
/// silent mis-price) for any future non-Anthropic row.
pub(crate) fn estimate_cost_usd(
    sheet: &PriceSheet,
    as_of: DateTime<Utc>,
    model: &str,
    input_tokens: i64,
    output_tokens: i64,
    cache_creation_tokens: i64,
    cache_read_tokens: i64,
) -> f64 {
    let r = sheet.rate("anthropic", model, as_of);
    (input_tokens as f64 / 1_000_000.0) * r.input_per_million
        + (output_tokens as f64 / 1_000_000.0) * r.output_per_million
        + (cache_creation_tokens as f64 / 1_000_000.0) * r.input_per_million * r.cache_write_multiplier
        + (cache_read_tokens as f64 / 1_000_000.0) * r.input_per_million * r.cache_read_multiplier
}

fn service_unavailable() -> Response {
    (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "service unavailable"}))).into_response()
}

fn internal_error(what: &str, e: impl std::fmt::Display) -> Response {
    tracing::error!(error = %e, query = what, "usage summary: query failed, refusing to fabricate a value");
    (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"}))).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn month_start_truncates_to_the_first_of_the_month_at_midnight_utc() {
        let mid_month = Utc.with_ymd_and_hms(2026, 8, 13, 17, 42, 9).unwrap();
        assert_eq!(month_start(mid_month), Utc.with_ymd_and_hms(2026, 8, 1, 0, 0, 0).unwrap());
    }

    #[test]
    fn month_start_is_idempotent_on_the_first_of_the_month() {
        let first = Utc.with_ymd_and_hms(2026, 8, 1, 0, 0, 0).unwrap();
        assert_eq!(month_start(first), first);
    }

    #[test]
    fn month_start_handles_the_december_to_january_rollover() {
        let dec = Utc.with_ymd_and_hms(2025, 12, 31, 23, 59, 59).unwrap();
        assert_eq!(month_start(dec), Utc.with_ymd_and_hms(2025, 12, 1, 0, 0, 0).unwrap());
    }

    /// Reuses `crate::billing::rates::tests::seeded_for_tests` — the same
    /// seed data `model_rates.sql` inserts — so this exercises the exact
    /// formula, not a hand-picked rate. Mirrors `workers::billing_derivation`'s
    /// own `estimate_cost_usd`, which is `#[cfg(test)]`-inaccessible from
    /// here (private to that module), so this is a deliberate, small,
    /// independently-verified second copy — see this module's own doc
    /// comment ("Cost estimate, not the ledger's own number") for why this
    /// handler prices from tokens directly instead of importing that
    /// worker's internals.
    #[test]
    fn estimate_cost_usd_matches_hand_computed_sonnet_5_pricing() {
        let sheet = crate::billing::rates::tests::seeded_for_tests();
        let as_of = Utc.with_ymd_and_hms(2026, 8, 1, 0, 0, 0).unwrap(); // intro pricing: 2.0 / 10.0 per million
        let cost = estimate_cost_usd(&sheet, as_of, "claude-sonnet-5", 1_000_000, 1_000_000, 0, 0);
        assert!((cost - 12.0).abs() < 1e-9, "expected $2 input + $10 output = $12, got {cost}");
    }

    #[test]
    fn estimate_cost_usd_prices_cache_tokens_off_the_input_rate_with_multipliers() {
        let sheet = crate::billing::rates::tests::seeded_for_tests();
        let as_of = Utc.with_ymd_and_hms(2026, 8, 1, 0, 0, 0).unwrap();
        // claude-haiku-4-5: input 1.0/million, cache_write x1.25, cache_read x0.1 (defaults).
        let cost = estimate_cost_usd(&sheet, as_of, "claude-haiku-4-5", 0, 0, 1_000_000, 1_000_000);
        assert!((cost - 1.35).abs() < 1e-9, "expected $1.25 cache-write + $0.10 cache-read = $1.35, got {cost}");
    }

    #[test]
    fn estimate_cost_usd_is_zero_for_zero_tokens() {
        let sheet = crate::billing::rates::tests::seeded_for_tests();
        let as_of = Utc.with_ymd_and_hms(2026, 8, 1, 0, 0, 0).unwrap();
        assert_eq!(estimate_cost_usd(&sheet, as_of, "claude-sonnet-5", 0, 0, 0, 0), 0.0);
    }
}

// ── Live-Postgres tests ──────────────────────────────────────────────────
//
// Gated on `RELINFER_TEST_DATABASE_URL`, same convention as
// `workers::billing_derivation::pg_tests` (see that module for the
// precedent this file follows) and `llm::usage::pg_tests`. `cargo test
// --lib` with no env var set SKIPS these with an `eprintln!` marker rather
// than failing. Every test mints a FRESH `Uuid::new_v4()` account, so tests
// never collide and need no shared-fixture cleanup.
//
// Calls `get_usage_summary` DIRECTLY with hand-built extractors rather than
// through a real HTTP router + minted Clerk JWT (contrast
// `journal_endpoint_pg.rs`'s heavier harness) — this handler's only
// interesting behavior is what it reads and how it shapes the response, not
// auth/routing, and `Tenant`/`UserContext` are both plain structs with
// public fields, so building them by hand is the smaller, equally-faithful
// proof.
//
// Mostly Consumer/Personal plane, same disclaimer as
// `workers::billing_derivation::pg_tests`: the CI job this suite rides in
// (`mcp-e2e`) applies only `migrations_control`, with no tenant-tree
// database. The two settings-launch-2.6 tests at the bottom DO drive the
// Org branch, by binding a `TenancyDecision::Org` whose tenant pool is the
// control database (every content table this handler reads exists on both
// trees, and CI connects as superuser, so the system-scoped read runs for
// real). What they prove is the admin gate and the org-wide shape of an
// admin's answer — not tenant-DB RLS, which still has no live coverage here.
#[cfg(test)]
mod pg_tests {
    use super::*;
    use crate::tenancy::extractor::Tenant;
    use crate::workers::billing_derivation::{ENTRY_KIND_JOB, ENTRY_KIND_QUERY};

    async fn pg_test_pool(test_name: &str) -> Option<sqlx::PgPool> {
        let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => u,
            Err(_) => {
                eprintln!("skipping {test_name}: set RELINFER_TEST_DATABASE_URL to run it");
                return None;
            }
        };
        Some(
            sqlx::postgres::PgPoolOptions::new()
                .max_connections(4)
                .connect(&url)
                .await
                .expect("connect to test Postgres"),
        )
    }

    /// A real `users` + `accounts` row pair — every table this handler reads
    /// FKs to `accounts(id)` (directly or via `graphs`/`query_history`), so
    /// every test needs one.
    async fn seed_account(pool: &sqlx::PgPool) -> Uuid {
        let user_id = Uuid::new_v4();
        let account_id = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(user_id)
            .bind(format!("{user_id}@usage-pr12.test"))
            .execute(pool)
            .await
            .expect("insert user");
        sqlx::query("INSERT INTO accounts (id, owner_user_id) VALUES ($1, $2)")
            .bind(account_id)
            .bind(user_id)
            .execute(pool)
            .await
            .expect("insert account");
        account_id
    }

    fn user_context(account_id: Uuid) -> UserContext {
        UserContext {
            user_id: Uuid::new_v4(),
            account_id,
            role: "owner".to_string(),
            device_id: "usage-pr12-test".to_string(),
        }
    }

    /// Personal/consumer-plane `Tenant`: content-plane pool IS the control
    /// pool (no tenant DB in this job — see the module doc), `decision:
    /// None` matches `Tenant::legacy`'s own "gate never ran / Personal"
    /// convention, which `get_usage_summary`'s `is_org` check treats
    /// identically to `Some(TenancyDecision::Personal)`.
    fn personal_tenant(pool: sqlx::PgPool) -> Tenant {
        Tenant {
            store: None,
            pool: Some(pool),
            decision: None,
            class: crate::route_class::RouteClass::Other,
        }
    }

    async fn call(state: Arc<AppState>, tenant: Tenant, user: UserContext) -> UsageSummaryResponse {
        let resp = get_usage_summary(State(state), tenant, Extension(user)).await;
        assert_eq!(resp.status(), StatusCode::OK, "expected 200, got a non-OK response");
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.expect("read body");
        serde_json::from_slice(&bytes).expect("valid UsageSummaryResponse JSON")
    }

    /// **The required negative control.** An account with ZERO rows in
    /// `credit_ledger`/`credit_balances`/`llm_usage`/`query_history` must
    /// render an HONEST empty state: 200 (not a 500/crash), every number a
    /// real zero (not a fabricated placeholder), `plan: None` (no grant
    /// exists — PR10 hasn't shipped in this tree), `by_workspace`/
    /// `top_askers` empty (not silently populated with something), and
    /// `has_ledger_history: false` (the signal the client uses to render the
    /// empty state instead of a zero-value dashboard — see
    /// `UsageSummaryView`'s own `isEmpty` check). This is the entire reason
    /// this PR exists: the page it replaces showed a confident `$186`
    /// regardless of what was actually in the database.
    #[tokio::test]
    async fn usage_summary_is_an_honest_empty_state_for_a_never_metered_account() {
        let Some(pool) = pg_test_pool("usage_summary_is_an_honest_empty_state_for_a_never_metered_account").await
        else {
            return;
        };
        let account_id = seed_account(&pool).await;
        let state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));

        let body = call(state, personal_tenant(pool), user_context(account_id)).await;

        assert_eq!(body.balance_credits, 0.0, "no credit_balances row -> honest zero, not fabricated");
        assert!(body.plan.is_none(), "no grants row exists in this tree yet -> honest None, not a default plan");
        assert_eq!(body.spend.total_credits, 0.0);
        assert_eq!(body.spend.total_cost_usd, 0.0);
        assert!(body.spend.by_axis.is_empty(), "no ledger rows -> empty axis list, not a fabricated zero row");
        assert!(body.by_workspace.is_empty());
        assert!(body.top_askers.is_empty());
        assert!(!body.has_ledger_history, "credit_ledger has never had a row for this account");
        assert!(
            body.ledger_coverage_start.is_none(),
            "no ledger row -> no coverage floor; None and has_ledger_history:false must agree"
        );
    }

    /// **The regression test for the self-contradicting usage screen.** An
    /// account with real `llm_usage` this period but NO `credit_ledger` rows
    /// (every account's state in the first month of metering — the ledger
    /// started at deploy with no backfill) used to serialize as `spend: 0` /
    /// `by_workspace: $X` with nothing in the payload explaining the gap, so
    /// the page rendered "$0.00" and "No metered activity this period" beside
    /// a populated by-workspace breakdown. Each number was individually
    /// correct and the screen was still incoherent.
    ///
    /// This asserts the payload carries what the client needs to reconcile
    /// them: raw usage present, ledger spend genuinely zero, and
    /// `ledger_coverage_start: None` marking that zero as "not billed yet"
    /// rather than "nothing happened". `web/lib/usageSummary.test.ts` pins
    /// the copy the client derives from exactly this combination.
    #[tokio::test]
    async fn usage_summary_marks_unbilled_llm_usage_instead_of_a_bare_zero() {
        let Some(pool) = pg_test_pool("usage_summary_marks_unbilled_llm_usage_instead_of_a_bare_zero").await
        else {
            return;
        };
        let account_id = seed_account(&pool).await;
        // Real spend this period, priced from real tokens — but nothing ever
        // ledgered for this account, exactly like a pre-epoch row.
        sqlx::query(
            "INSERT INTO llm_usage (account_id, graph_id, task, model, input_tokens, output_tokens, created_at) \
             VALUES ($1, NULL, 'synthesize', 'claude-sonnet-5', 2000000, 500000, now())",
        )
        .bind(account_id)
        .execute(&pool)
        .await
        .expect("insert llm_usage row with no matching ledger entry");

        let state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
        let body = call(state, personal_tenant(pool), user_context(account_id)).await;

        let raw_usd: f64 = body.by_workspace.iter().map(|w| w.cost_usd).sum();
        assert!(raw_usd > 0.0, "the llm_usage row must still be priced and shown, not dropped");
        assert_eq!(body.spend.total_credits, 0.0, "nothing was ledgered, so ledger spend is a real zero");
        assert!(body.spend.by_axis.is_empty());
        assert!(
            body.ledger_coverage_start.is_none(),
            "the zero above is only honest because this field says billing data starts nowhere yet"
        );
    }

    /// `ledger_coverage_start` is the account's OLDEST ledger row ever, and is
    /// deliberately NOT period-scoped (unlike `spend`) — it answers "from when
    /// is this account billed at all", which is the question a mid-period
    /// epoch makes the client ask. One row well before this month and one
    /// inside it proves both halves in a single call: the coverage floor
    /// reaches back past `period_start` while `spend` still counts only what
    /// falls inside the window.
    #[tokio::test]
    async fn ledger_coverage_start_is_the_oldest_row_ever_while_spend_stays_period_scoped() {
        let Some(pool) =
            pg_test_pool("ledger_coverage_start_is_the_oldest_row_ever_while_spend_stays_period_scoped").await
        else {
            return;
        };
        let account_id = seed_account(&pool).await;
        // Seed the original read controls through the real account/period
        // financial entry; every guarded write must use this same transaction.
        let mut tx = crate::tenancy::scope::begin_account_scoped(&pool, account_id)
            .await.expect("scope usage fixture account");
        crate::billing::renewal::lock_period_rotation(&mut tx, account_id)
            .await.expect("enter compatible financial transaction");
        // Deliberately relative to `now()` rather than a fixed date, so this
        // test cannot start passing or failing based on the month it runs in.
        sqlx::query(
            "INSERT INTO credit_ledger (account_id, entry_kind, credits, cost_usd, source_job_id, created_at) \
             VALUES ($1, $2, -9.0, 0.05, $3, now() - interval '400 days')",
        )
        .bind(account_id)
        .bind(ENTRY_KIND_JOB)
        .bind(Uuid::new_v4())
        .execute(&mut *tx)
        .await
        .expect("insert long-past ledger row");
        sqlx::query(
            "INSERT INTO credit_ledger (account_id, entry_kind, credits, cost_usd, source_query_id, created_at) \
             VALUES ($1, $2, -2.0, 0.01, $3, now())",
        )
        .bind(account_id)
        .bind(ENTRY_KIND_QUERY)
        .bind(Uuid::new_v4())
        .execute(&mut *tx)
        .await
        .expect("insert current-period ledger row");

        tx.commit().await.expect("commit usage financial fixtures");

        let state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
        let body = call(state, personal_tenant(pool), user_context(account_id)).await;

        let coverage = body.ledger_coverage_start.expect("two ledger rows exist -> a coverage floor exists");
        assert!(
            coverage < body.period_start,
            "the floor must be the 400-day-old row ({coverage}), not the one inside this period"
        );
        assert!(body.has_ledger_history);
        assert!(
            (body.spend.total_credits - 2.0).abs() < 1e-9,
            "only the in-period row counts toward spend; the 400-day-old one must not, got {}",
            body.spend.total_credits
        );
    }

    /// Positive control alongside the negative one above: real
    /// `credit_ledger`/`credit_balances` rows for this account produce the
    /// real numbers, not a coincidental zero. Two axes (job + query) prove
    /// `by_axis` groups correctly and `total_credits`/`total_cost_usd` sum
    /// across both.
    #[tokio::test]
    async fn usage_summary_reflects_real_ledger_rows_this_period() {
        let Some(pool) = pg_test_pool("usage_summary_reflects_real_ledger_rows_this_period").await else {
            return;
        };
        let account_id = seed_account(&pool).await;
        // Seed the original read controls through the real account/period
        // financial entry; every guarded write must use this same transaction.
        let mut tx = crate::tenancy::scope::begin_account_scoped(&pool, account_id)
            .await.expect("scope usage fixture account");
        crate::billing::renewal::lock_period_rotation(&mut tx, account_id)
            .await.expect("enter compatible financial transaction");
        sqlx::query("INSERT INTO credit_balances (account_id, balance) VALUES ($1, 42.5)")
            .bind(account_id)
            .execute(&mut *tx)
            .await
            .expect("insert balance");
        sqlx::query(
            "INSERT INTO credit_ledger (account_id, entry_kind, credits, cost_usd, source_job_id, created_at) \
             VALUES ($1, $2, -3.0, 0.0132, $3, now())",
        )
        .bind(account_id)
        .bind(ENTRY_KIND_JOB)
        .bind(Uuid::new_v4())
        .execute(&mut *tx)
        .await
        .expect("insert job-axis ledger row");
        sqlx::query(
            "INSERT INTO credit_ledger (account_id, entry_kind, credits, cost_usd, source_query_id, created_at) \
             VALUES ($1, $2, -1.0, 0.0044, $3, now())",
        )
        .bind(account_id)
        .bind(ENTRY_KIND_QUERY)
        .bind(Uuid::new_v4())
        .execute(&mut *tx)
        .await
        .expect("insert query-axis ledger row");

        tx.commit().await.expect("commit usage financial fixtures");

        let state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
        let body = call(state, personal_tenant(pool), user_context(account_id)).await;

        assert_eq!(body.balance_credits, 42.5);
        assert!(body.has_ledger_history);
        assert!((body.spend.total_credits - 4.0).abs() < 1e-9, "3.0 + 1.0 credits spent, got {}", body.spend.total_credits);
        assert!(
            (body.spend.total_cost_usd - 0.0176).abs() < 1e-9,
            "0.0132 + 0.0044 usd, got {}",
            body.spend.total_cost_usd
        );
        assert_eq!(body.spend.by_axis.len(), 2, "job and query axes must not collapse into one bucket");
    }

    /// **The unattributed-workspace requirement.** A `llm_usage` row with no
    /// `graph_id` must surface as an explicit `unattributed: true` bucket in
    /// `by_workspace` — never silently dropped (the plan's own framing: a
    /// usage page that drops rows under-reports, which is the same class of
    /// bug this PR exists to fix).
    #[tokio::test]
    async fn usage_summary_shows_an_unattributed_bucket_for_null_graph_id_llm_usage_rows() {
        let Some(pool) =
            pg_test_pool("usage_summary_shows_an_unattributed_bucket_for_null_graph_id_llm_usage_rows").await
        else {
            return;
        };
        let account_id = seed_account(&pool).await;
        sqlx::query(
            "INSERT INTO llm_usage (account_id, graph_id, task, model, input_tokens, output_tokens, created_at) \
             VALUES ($1, NULL, 'extract_entities', 'claude-haiku-4-5', 1000000, 0, now())",
        )
        .bind(account_id)
        .execute(&pool)
        .await
        .expect("insert graph_id-less llm_usage row");

        let state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
        let body = call(state, personal_tenant(pool), user_context(account_id)).await;

        assert_eq!(body.by_workspace.len(), 1);
        let bucket = &body.by_workspace[0];
        assert!(bucket.unattributed, "a NULL-graph_id row must land in the unattributed bucket, not vanish");
        assert!(bucket.graph_id.is_none());
        assert!(bucket.cost_usd > 0.0, "the row's real tokens must still be priced, not zeroed out");
    }

    /// **`atoms_ingested`**: counts by ingestion time (`atoms.created_at`),
    /// not the source event's own `timestamp` — a row backdated to last year
    /// but ingested THIS period must still count, and a row with a recent
    /// `timestamp` ingested BEFORE `period_start` must not. `atoms` carries
    /// no `account_id` of its own, so this also proves the `graphs` join
    /// scopes correctly: a second account's graph/atom must never leak in.
    #[tokio::test]
    async fn atoms_ingested_counts_by_ingestion_time_and_stays_account_scoped() {
        let Some(pool) = pg_test_pool("atoms_ingested_counts_by_ingestion_time_and_stays_account_scoped").await else {
            return;
        };
        let account_id = seed_account(&pool).await;
        let other_account_id = seed_account(&pool).await;
        let graph_id = Uuid::new_v4();
        let other_graph_id = Uuid::new_v4();
        sqlx::query("INSERT INTO graphs (id, account_id, name) VALUES ($1, $2, 'test ocean')")
            .bind(graph_id)
            .bind(account_id)
            .execute(&pool)
            .await
            .expect("insert graph");
        sqlx::query("INSERT INTO graphs (id, account_id, name) VALUES ($1, $2, 'other account ocean')")
            .bind(other_graph_id)
            .bind(other_account_id)
            .execute(&pool)
            .await
            .expect("insert other account's graph");

        // In-period, this account: counts.
        sqlx::query(
            "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, \
                 \"timestamp\", created_at) \
             VALUES ($1, 'manual', 'in-period', 'blob-1', now() - interval '400 days', now())",
        )
        .bind(graph_id)
        .execute(&pool)
        .await
        .expect("insert in-period atom, backdated source timestamp");
        // Ingested BEFORE this period (even though its source timestamp is
        // recent): must not count.
        sqlx::query(
            "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, \
                 \"timestamp\", created_at) \
             VALUES ($1, 'manual', 'pre-period', 'blob-2', now(), now() - interval '400 days')",
        )
        .bind(graph_id)
        .execute(&pool)
        .await
        .expect("insert pre-period atom, recent source timestamp");
        // Another account's atom: must never leak into this account's count.
        sqlx::query(
            "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, \
                 \"timestamp\", created_at) \
             VALUES ($1, 'manual', 'other-account', 'blob-3', now(), now())",
        )
        .bind(other_graph_id)
        .execute(&pool)
        .await
        .expect("insert other account's atom");

        let state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
        let body = call(state, personal_tenant(pool), user_context(account_id)).await;

        assert_eq!(
            body.atoms_ingested, 1,
            "exactly the one in-period, this-account atom must count"
        );
    }

    // ── Settings launch PR 2.6: org branch is admin-only ────────────────

    /// One org account with real org-wide spend: a ledgered debit carrying
    /// dollars, a priced `llm_usage` row on one of its workspaces, and a
    /// `query_history` row from a second user so `top_askers` has someone
    /// (with an email) to leak. Returns `(account_id, asker_user_id, graph_id)`.
    async fn seed_org_with_spend(pool: &sqlx::PgPool) -> (Uuid, Uuid, Uuid) {
        let account_id = seed_account(pool).await;
        let asker_id = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(asker_id)
            .bind(format!("{asker_id}@usage-2-6-asker.test"))
            .execute(pool)
            .await
            .expect("insert asker user");
        let graph_id = Uuid::new_v4();
        sqlx::query("INSERT INTO graphs (id, account_id, name) VALUES ($1, $2, 'org workspace 2.6')")
            .bind(graph_id)
            .bind(account_id)
            .execute(pool)
            .await
            .expect("insert org graph");
        sqlx::query(
            "INSERT INTO query_history (account_id, graph_id, user_id, query_text) \
             VALUES ($1, $2, $3, 'who spent what')",
        )
        .bind(account_id)
        .bind(graph_id)
        .bind(asker_id)
        .execute(pool)
        .await
        .expect("insert query_history");
        sqlx::query(
            "INSERT INTO llm_usage (account_id, graph_id, task, model, input_tokens, output_tokens, created_at) \
             VALUES ($1, $2, 'synthesize', 'claude-sonnet-5', 1000000, 1000000, now())",
        )
        .bind(account_id)
        .bind(graph_id)
        .execute(pool)
        .await
        .expect("insert org llm_usage");
        let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id)
            .await.expect("scope org usage fixture account");
        crate::billing::renewal::lock_period_rotation(&mut tx, account_id)
            .await.expect("enter compatible financial transaction");
        sqlx::query(
            "INSERT INTO credit_ledger (account_id, entry_kind, credits, cost_usd, source_query_id, created_at) \
             VALUES ($1, $2, -5.0, 0.25, $3, now())",
        )
        .bind(account_id)
        .bind(ENTRY_KIND_QUERY)
        .bind(Uuid::new_v4())
        .execute(&mut *tx)
        .await
        .expect("insert org ledger debit");
        tx.commit().await.expect("commit org financial fixture");
        (account_id, asker_id, graph_id)
    }

    /// An org-bound `Tenant` whose tenant pool is the control database —
    /// see the suite header for why that is a faithful stand-in here.
    fn org_tenant(pool: sqlx::PgPool, account_id: Uuid, user_id: Uuid) -> Tenant {
        Tenant {
            store: None,
            pool: Some(pool.clone()),
            decision: Some(TenancyDecision::Org(
                crate::tenancy::runtime::TenantContext {
                    org_id: format!("org_usage_2_6_{account_id}"),
                    account_id,
                    user_id,
                    pool,
                    page_pool: None,
                },
            )),
            class: crate::route_class::RouteClass::Other,
        }
    }

    fn org_user(account_id: Uuid, user_id: Uuid, role: &str) -> UserContext {
        UserContext {
            user_id,
            account_id,
            role: role.to_string(),
            device_id: "usage-2-6-test".to_string(),
        }
    }

    /// **The leak this PR closes.** A plain org member (`role = "member"`,
    /// what `clerk_provision::map_org_role` gives every non-admin Clerk
    /// role) must get a 403 carrying none of the org-wide document: no
    /// `spend`/`total_cost_usd`, no `by_workspace` cost, no `top_askers`
    /// emails — even though the org has real spend and a real asker the
    /// admin test below proves would otherwise be returned. Before 2.6 this
    /// exact call returned 200 with all of it.
    #[tokio::test]
    async fn org_member_is_refused_org_wide_dollars_and_top_askers() {
        let Some(pool) = pg_test_pool("org_member_is_refused_org_wide_dollars_and_top_askers").await else {
            return;
        };
        let (account_id, asker_id, _graph_id) = seed_org_with_spend(&pool).await;
        let state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));

        // The asker themself is the member here: even your OWN row in
        // `top_askers` sits inside an org-wide document you may not read.
        let resp = get_usage_summary(
            State(state),
            org_tenant(pool, account_id, asker_id),
            Extension(org_user(account_id, asker_id, "member")),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::FORBIDDEN, "a plain org member must be refused");
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.expect("read body");
        let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON error body");
        assert_eq!(body, json!({"error": "admin only"}), "the refusal must carry nothing but the error");
        let raw = String::from_utf8_lossy(&bytes);
        for leaked in ["total_cost_usd", "cost_usd", "top_askers", "by_workspace", "balance_credits", "usage-2-6-asker"] {
            assert!(!raw.contains(leaked), "member response leaked `{leaked}`: {raw}");
        }
    }

    /// The positive control the member test needs to mean anything: the
    /// SAME seeded org, read by an `admin` (and by an `owner`, which
    /// `org_invites::is_admin` also accepts), still gets the full org-wide
    /// document — the ledgered dollars, the priced workspace, and the
    /// asker by email.
    #[tokio::test]
    async fn org_admin_still_gets_org_wide_dollars_and_top_askers() {
        let Some(pool) = pg_test_pool("org_admin_still_gets_org_wide_dollars_and_top_askers").await else {
            return;
        };
        let (account_id, asker_id, graph_id) = seed_org_with_spend(&pool).await;

        for role in ["admin", "owner"] {
            let admin_id = Uuid::new_v4();
            let state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
            let body = call(
                state,
                org_tenant(pool.clone(), account_id, admin_id),
                org_user(account_id, admin_id, role),
            )
            .await;

            assert!(
                (body.spend.total_cost_usd - 0.25).abs() < 1e-9,
                "{role}: the org's ledgered dollars must be returned, got {}",
                body.spend.total_cost_usd
            );
            assert!((body.spend.total_credits - 5.0).abs() < 1e-9, "{role}: credits spent");
            let ws = body
                .by_workspace
                .iter()
                .find(|w| w.graph_id == Some(graph_id))
                .unwrap_or_else(|| panic!("{role}: the org workspace must appear in by_workspace"));
            assert!(ws.cost_usd > 0.0, "{role}: the workspace's spend must be priced");
            let asker = body
                .top_askers
                .iter()
                .find(|a| a.user_id == Some(asker_id))
                .unwrap_or_else(|| panic!("{role}: the asker must appear in top_askers"));
            assert_eq!(asker.asks, 1);
            assert_eq!(asker.email.as_deref(), Some(format!("{asker_id}@usage-2-6-asker.test").as_str()));
        }
    }
}
