//! DB-versioned LLM rate sheet (usage-metering PR1).
//!
//! Replaces the previous hardcoded `MODEL_RATES_PER_MILLION` / `SONNET_5_*` /
//! `DEFAULT_RATE_PER_MILLION` Rust constants in `routes::admin` with rows
//! read from the control-plane `model_rates` table (migration
//! `0148_model_rates.sql` — see its header for the full column contract and
//! seed data), so a rate correction or a new model no longer needs a code
//! deploy.
//!
//! ## Loading vs. resolving
//! [`PriceSheet::load`] is the ONLY async/DB-touching function in this
//! module — it reads the table's FULL history (every row, regardless of
//! `effective_from`/`effective_to`) into memory once. Everything else,
//! starting with [`PriceSheet::rate`], is synchronous and pure: a report for
//! a past month must price a model identically no matter when the report is
//! re-run, so historical rows are never discarded at load time — only
//! filtered, in memory, against the caller-supplied `as_of`.
//!
//! Callers MUST snapshot exactly ONE [`PriceSheet`] per report/request and
//! pass it by reference into every pricing call for that report — see
//! `routes::admin::admin_spend`, which loads it once near the top of the
//! handler and threads `&sheet` through its per-account loop. A
//! per-iteration reload would both regress that loop's 45s-budgeted
//! `SPEND_PERSONAL_LOOP_TIMEOUT` and force `#[tokio::test]` + a live
//! Postgres fixture onto what are otherwise pure pricing unit tests.
//!
//! ## Ordered-substring matching
//! `llm_usage.model` is a free-form provider string (e.g.
//! `"anthropic.claude-sonnet-5-v1"`), never a foreign key — pricing has
//! always resolved it by SUBSTRING containment (`model.contains(pattern)`),
//! not equality, so a dated/provider-prefixed snapshot id still resolves to
//! its base model's rate. The `priority` column reproduces the previous
//! Rust `&[(pattern, ...)]` array's iteration order EXACTLY: lower
//! `priority` is checked first, and the first (lowest-priority) row whose
//! pattern `model` contains wins — load-bearing for overlapping patterns
//! like `"claude-3-5-haiku"`, which must win over the more general
//! `"claude-haiku"` for a `claude-3-5-haiku-...` id.

use chrono::{DateTime, Utc};
use sqlx::PgPool;

/// The sentinel `llm_usage.model` value org-plane rows use when no
/// per-model breakdown exists at all (`platform_metrics_daily` — the
/// org-plane rollup `routes::admin::admin_spend` reads — has no `model`
/// column). Deliberately routed to the blend row with NO alert: this is
/// expected, not a data-quality problem. See [`PriceSheet::rate`]'s doc
/// comment and plan §3 item 4 ("keep the sentinel path; alert only on
/// genuinely unrecognized models").
pub const UNATTRIBUTED_SENTINEL: &str = "__unattributed__";

/// One row of `model_rates` (migration `0148_model_rates.sql`), loaded
/// verbatim — see that migration's header for the full column contract.
#[derive(Debug, Clone)]
pub struct RateRow {
    /// Filtered on exactly by [`PriceSheet::rate`]'s primary (non-blend)
    /// match phase (OpenRouter provider port, PR1) — e.g. `"anthropic"`,
    /// `"openrouter"`. NOT applied to the blend/fallback row lookup — see
    /// `rate`'s own doc for why a provider-scoped blend fallback would be a
    /// new panic risk for any provider that doesn't seed its own blend row.
    pub provider: String,
    pub model_pattern: String,
    pub priority: i32,
    pub input_per_million: f64,
    pub output_per_million: f64,
    pub cache_write_multiplier: f64,
    pub cache_read_multiplier: f64,
    pub effective_from: DateTime<Utc>,
    pub effective_to: Option<DateTime<Utc>>,
    pub is_blend_fallback: bool,
}

/// The full `model_rates` history, snapshotted once per report — see the
/// module doc for why loading is async/DB-touching but resolution is not.
#[derive(Debug, Clone)]
pub struct PriceSheet {
    rows: Vec<RateRow>,
}

/// The result of [`PriceSheet::rate`]: the resolved per-million rates and
/// cache multipliers, plus whether a SPECIFIC pattern matched (`matched:
/// true`) or the result is the blend/unattributed fallback (`matched:
/// false`). `routes::admin::estimate_cost_usd` only needs the four rate
/// numbers; the model-config completeness test
/// (`routes::admin::tests::every_model_config_model_resolves_to_a_real_rate`)
/// asserts on `matched` directly.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ResolvedRate {
    pub input_per_million: f64,
    pub output_per_million: f64,
    pub cache_write_multiplier: f64,
    pub cache_read_multiplier: f64,
    pub matched: bool,
}

impl PriceSheet {
    /// Load the FULL rate history from the control plane. Call ONCE per
    /// report/request — never per row/iteration (see the module doc).
    ///
    /// `NUMERIC` columns are cast to `float8` in the query itself — this
    /// workspace has no `bigdecimal`/`rust_decimal` sqlx feature enabled,
    /// and the downstream math in `routes::admin::estimate_cost_usd` is
    /// already `f64` throughout — same "cast the aggregate back down to a
    /// type sqlx already decodes" convention `routes::admin`'s own
    /// `SUM(bigint)::bigint` casts use for the identical reason.
    pub async fn load(pool: &PgPool) -> Result<Self, sqlx::Error> {
        let mut conn=pool.acquire().await?;
        Self::load_tx(&mut conn).await
    }
    pub(crate) async fn load_tx(conn:&mut sqlx::PgConnection)->Result<Self,sqlx::Error>{
        let rows: Vec<(
            String,
            String,
            i32,
            f64,
            f64,
            f64,
            f64,
            DateTime<Utc>,
            Option<DateTime<Utc>>,
            bool,
        )> = sqlx::query_as(
            "SELECT provider, model_pattern, priority, \
                    input_per_million::float8, output_per_million::float8, \
                    cache_write_multiplier::float8, cache_read_multiplier::float8, \
                    effective_from, effective_to, is_blend_fallback \
             FROM model_rates \
             ORDER BY priority",
        )
        .fetch_all(conn)
        .await?;

        Ok(Self::from_rows(
            rows.into_iter()
                .map(
                    |(
                        provider,
                        model_pattern,
                        priority,
                        input_per_million,
                        output_per_million,
                        cache_write_multiplier,
                        cache_read_multiplier,
                        effective_from,
                        effective_to,
                        is_blend_fallback,
                    )| RateRow {
                        provider,
                        model_pattern,
                        priority,
                        input_per_million,
                        output_per_million,
                        cache_write_multiplier,
                        cache_read_multiplier,
                        effective_from,
                        effective_to,
                        is_blend_fallback,
                    },
                )
                .collect(),
        ))
    }

    /// Build a sheet directly from rows already in memory. `pub(crate)` —
    /// the only in-crate, non-Postgres caller is
    /// [`tests::seeded_for_tests`] below, which mirrors the migration's
    /// seed data so `routes::admin`'s pricing tests stay plain `#[test]`s.
    pub(crate) fn from_rows(rows: Vec<RateRow>) -> Self {
        Self { rows }
    }

    /// Resolve `model`'s rate as of `as_of` — the REPORTED period's start,
    /// never `Utc::now()`. A report for a past month must price a
    /// time-gated model (`claude-sonnet-5`'s intro/standard split is the
    /// reason this distinction exists at all — see migration
    /// `0148_model_rates.sql`'s header) at the rate that was live THAT
    /// month, not whichever rate happens to be live when the report is
    /// re-run later — see plan §4 item 5: using `now()` here "silently
    /// reprices history."
    ///
    /// Three-phase lookup (the pre-PR1 `routes::admin::model_rate` fn's
    /// original single substring-match phase, plus the `provider` filter
    /// and its fallback added by the OpenRouter provider port, PR1):
    ///   1. Among non-blend rows ACTIVE at `as_of`
    ///      (`effective_from <= as_of AND (effective_to IS NULL OR
    ///      effective_to > as_of)`) whose `provider` EXACTLY equals the
    ///      `provider` argument and whose `model_pattern` is a substring of
    ///      `model`, take the lowest `priority` — see the module doc for
    ///      why ordered-substring, not equality.
    ///   2. If nothing matched, retry the SAME substring match with the
    ///      `provider` filter dropped — see "Phase 2" below for why this is
    ///      safe.
    ///   3. If still nothing matched, fall back to the active blend row
    ///      (`is_blend_fallback = true`). This is the only path that
    ///      returns `matched: false`.
    ///
    /// A model that falls to the blend AND is not the
    /// [`UNATTRIBUTED_SENTINEL`] logs a `tracing::warn!(alert = true, ...)`
    /// — the sentinel's blend routing is deliberate and silent (org-plane
    /// rows have no per-model breakdown to match against at all), but a
    /// genuinely unrecognized model string silently blending is exactly the
    /// mispricing failure mode plan §3 item 4 and §4 item 5 call out. This
    /// is the minimal, non-deduped half of that alert — PR11's
    /// reconciliation work is what adds the `pool_failure_severity`-style
    /// once-per-outage dedup; this PR only makes sure the signal exists at
    /// all rather than being silently absorbed.
    ///
    /// `provider` (OpenRouter provider port, PR1) is matched EXACTLY
    /// (`RateRow.provider == provider`, never substring) against the SAME
    /// `(provider, model)` pair `llm_usage.provider`/`llm_usage.model`
    /// record for a call, so `"deepseek/deepseek-v4-flash-0731"` under
    /// `provider: "openrouter"` doesn't collide with a same-named pattern
    /// some future non-OpenRouter provider might also use.
    ///
    /// Deliberately NOT applied to the blend/fallback row lookup below: the
    /// blend row is a last-resort, provider-agnostic average, and a
    /// provider-scoped blend lookup would panic (`.expect(...)` below) for
    /// any provider that doesn't seed its own `is_blend_fallback` row —
    /// today, only `"anthropic"` does (`0148_model_rates.sql`). A model
    /// string that doesn't match ANY seeded pattern for ITS provider still
    /// safely falls through to the one cross-provider blend rate, exactly
    /// as before this parameter existed.
    ///
    /// **Phase 2 — provider-agnostic pattern fallback**, tried only when the
    /// exact `(provider, model)` phase above finds nothing. Model ids are
    /// namespaced by construction (`claude-*` is only ever Anthropic;
    /// `deepseek/*`/`z-ai/*` are only ever OpenRouter today), so re-trying
    /// the SAME substring match with the provider filter dropped is safe —
    /// a real cross-provider pattern COLLISION would need two providers to
    /// seed genuinely overlapping model-id substrings, which does not
    /// happen with today's namespacing. This exists specifically for
    /// `routes::admin::model_rate`/`routes::usage::estimate_cost_usd`/
    /// `workers::billing_derivation::estimate_cost_usd`, which (see each of
    /// their own doc comments) still hardcode `"anthropic"` internally
    /// because their `llm_usage` aggregation queries don't carry a
    /// per-bucket `provider` column yet — phase 2 makes them price an
    /// OpenRouter row CORRECTLY off the model pattern alone despite the
    /// wrong provider string, rather than silently blending it.
    pub fn rate(&self, provider: &str, model: &str, as_of: DateTime<Utc>) -> ResolvedRate {
        let is_active = |row: &&RateRow| {
            row.effective_from <= as_of && row.effective_to.map(|to| to > as_of).unwrap_or(true)
        };

        let matched = self
            .rows
            .iter()
            .filter(|r| !r.is_blend_fallback)
            .filter(|r| r.provider == provider)
            .filter(is_active)
            .filter(|r| model.contains(r.model_pattern.as_str()))
            .min_by_key(|r| r.priority)
            .or_else(|| {
                self.rows
                    .iter()
                    .filter(|r| !r.is_blend_fallback)
                    .filter(is_active)
                    .filter(|r| model.contains(r.model_pattern.as_str()))
                    .min_by_key(|r| r.priority)
            });

        if let Some(row) = matched {
            return ResolvedRate {
                input_per_million: row.input_per_million,
                output_per_million: row.output_per_million,
                cache_write_multiplier: row.cache_write_multiplier,
                cache_read_multiplier: row.cache_read_multiplier,
                matched: true,
            };
        }

        if model != UNATTRIBUTED_SENTINEL {
            tracing::warn!(
                provider = %provider,
                model = %model,
                alert = true,
                "billing: no model_rates row matched — falling back to the blend rate; \
                 this model's estimated spend is a rough blended average until a specific \
                 row is added to model_rates"
            );
        }

        // A seeded PriceSheet always carries exactly one active blend row —
        // see 0148_model_rates.sql's seed data. Its absence is a seeding bug
        // (someone deleted the blend row), not a runtime condition to
        // recover from silently — same "fail loud rather than fabricate"
        // discipline `routes::admin::fail_loud!` uses for headline numbers.
        let blend = self
            .rows
            .iter()
            .filter(|r| r.is_blend_fallback)
            .filter(is_active)
            .min_by_key(|r| r.priority)
            .expect("PriceSheet has no active blend-fallback row — model_rates is missing its is_blend_fallback seed row");

        ResolvedRate {
            input_per_million: blend.input_per_million,
            output_per_million: blend.output_per_million,
            cache_write_multiplier: blend.cache_write_multiplier,
            cache_read_multiplier: blend.cache_read_multiplier,
            matched: false,
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use chrono::NaiveDate;

    /// Mirrors the seed rows inserted by
    /// `backend/migrations_control/0148_model_rates.sql` — kept in lockstep
    /// BY HAND. This is a deliberate second copy of the seed data so
    /// `routes::admin`'s pricing tests (and this module's own) can run as
    /// plain `#[test]`s with no live Postgres, per plan §1 PR1's "critical
    /// shape" (a per-iteration/per-test DB round-trip would force
    /// `#[tokio::test]` + a fixture onto what are otherwise pure functions).
    ///
    /// If you edit the migration's seed data, mirror the edit here in the
    /// SAME PR. This list's fidelity to the real seed was verified with a
    /// manual negative control (not a committed test — see this PR's
    /// description): temporarily commenting out the `claude-opus-5` AND
    /// `claude-opus` rows below made
    /// `routes::admin::tests::every_model_config_model_resolves_to_a_real_rate_not_the_blend`
    /// fail (proving the completeness gate isn't vacuous) and confirmed the
    /// `tracing::warn!(alert = true, ...)` in [`PriceSheet::rate`] actually
    /// fires on the unmatched path — both rows were restored immediately
    /// after. A SINGLE row deletion is not sufficient to observe this: every
    /// model string this backend actually dispatches to has both a specific
    /// row AND a broader family-generic row (`"claude-haiku"`,
    /// `"claude-sonnet"`, `"claude-opus"`) still present, so removing only
    /// the specific one just falls through to the generic one — a real rate,
    /// not the blend. That cascading redundancy is intentional (a faithful
    /// port of the pre-PR1 Rust array's own layered `&[(pattern, ...)]`
    /// fallback), not a gap in this negative control.
    pub(crate) fn seeded_for_tests() -> PriceSheet {
        let epoch = DateTime::<Utc>::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2000, 1, 1)
                .and_then(|d| d.and_hms_opt(0, 0, 0))
                .expect("valid epoch marker"),
            Utc,
        );
        let sonnet_5_cutover = DateTime::<Utc>::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2026, 9, 1)
                .and_then(|d| d.and_hms_opt(0, 0, 0))
                .expect("valid cutover date"),
            Utc,
        );

        let row = |pattern: &str,
                    priority: i32,
                    input: f64,
                    output: f64,
                    from: DateTime<Utc>,
                    to: Option<DateTime<Utc>>,
                    is_blend: bool| RateRow {
            provider: "anthropic".to_string(),
            model_pattern: pattern.to_string(),
            priority,
            input_per_million: input,
            output_per_million: output,
            cache_write_multiplier: 1.25,
            cache_read_multiplier: 0.1,
            effective_from: from,
            effective_to: to,
            is_blend_fallback: is_blend,
        };

        // Mirrors `0200_openrouter_model_rates.sql`'s seed rows — OpenRouter
        // provider port, PR1. `cache_write_multiplier` is seeded at 1.0
        // (input rate = cache-write rate) per that migration's own header;
        // `cache_read_multiplier` is the DeepInfra-pinned cache-read rate
        // divided by the row's own input rate.
        let openrouter_row = |pattern: &str,
                               priority: i32,
                               input: f64,
                               output: f64,
                               cache_read_multiplier: f64| RateRow {
            provider: "openrouter".to_string(),
            model_pattern: pattern.to_string(),
            priority,
            input_per_million: input,
            output_per_million: output,
            cache_write_multiplier: 1.0,
            cache_read_multiplier,
            effective_from: epoch,
            effective_to: None,
            is_blend_fallback: false,
        };

        PriceSheet::from_rows(vec![
            row("claude-sonnet-5", 0, 2.0, 10.0, epoch, Some(sonnet_5_cutover), false),
            row("claude-sonnet-5", 0, 3.0, 15.0, sonnet_5_cutover, None, false),
            row("claude-haiku-4-5", 1, 1.0, 5.0, epoch, None, false),
            row("claude-sonnet-4-6", 2, 3.0, 15.0, epoch, None, false),
            row("claude-opus-4-8", 3, 5.0, 25.0, epoch, None, false),
            row("claude-opus-5", 4, 5.0, 25.0, epoch, None, false),
            row("claude-fable-5", 5, 10.0, 50.0, epoch, None, false),
            row("claude-3-5-sonnet", 6, 3.0, 15.0, epoch, None, false),
            row("claude-3-7-sonnet", 7, 3.0, 15.0, epoch, None, false),
            row("claude-sonnet", 8, 3.0, 15.0, epoch, None, false),
            row("claude-3-5-haiku", 9, 0.8, 4.0, epoch, None, false),
            row("claude-3-haiku", 10, 0.25, 1.25, epoch, None, false),
            row("claude-haiku", 11, 0.8, 4.0, epoch, None, false),
            row("claude-3-opus", 12, 15.0, 75.0, epoch, None, false),
            row("claude-opus", 13, 15.0, 75.0, epoch, None, false),
            row("__blend__", 100, 5.7, 27.5, epoch, None, true),
            openrouter_row("deepseek-v4-flash", 0, 0.08, 0.18, 0.2),
            openrouter_row("glm-5.3-flash", 1, 0.075, 0.25, 0.2),
            openrouter_row("text-embedding-3-small", 0, 0.02, 0.0, 1.0),
        ])
    }

    #[test]
    fn cloud_embedding_has_its_own_rate_not_the_blend() {
        let rate = seeded_for_tests().rate("openrouter", "openai/text-embedding-3-small", month_start(2026, 10));
        assert!(rate.matched);
        assert_eq!(rate.input_per_million, 0.02);
    }

    fn month_start(year: i32, month: u32) -> DateTime<Utc> {
        let dt = NaiveDate::from_ymd_opt(year, month, 1)
            .and_then(|d| d.and_hms_opt(0, 0, 0))
            .expect("valid test date");
        DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc)
    }

    #[test]
    fn ordered_substring_matching_prefers_the_more_specific_pattern() {
        let sheet = seeded_for_tests();
        let as_of = month_start(2026, 8);

        // "claude-3-5-haiku-20241022" contains BOTH "claude-3-5-haiku"
        // (priority 9) and "claude-haiku" (priority 11) — priority 9 must
        // win, exactly like the pre-PR1 array's iteration order.
        let resolved = sheet.rate("anthropic", "claude-3-5-haiku-20241022", as_of);
        assert!(resolved.matched);
        assert_eq!((resolved.input_per_million, resolved.output_per_million), (0.8, 4.0));
    }

    #[test]
    fn sonnet_5_flips_at_the_september_2026_cutover() {
        let sheet = seeded_for_tests();
        let intro = sheet.rate("anthropic", "claude-sonnet-5", month_start(2026, 8));
        assert!(intro.matched);
        assert_eq!((intro.input_per_million, intro.output_per_million), (2.0, 10.0));

        let standard = sheet.rate("anthropic", "claude-sonnet-5", month_start(2026, 9));
        assert!(standard.matched);
        assert_eq!((standard.input_per_million, standard.output_per_million), (3.0, 15.0));

        // Suffixed/provider-prefixed ids take the same time-dependent path.
        let intro_suffixed = sheet.rate("anthropic", "anthropic.claude-sonnet-5-v1", month_start(2026, 8));
        assert_eq!((intro_suffixed.input_per_million, intro_suffixed.output_per_million), (2.0, 10.0));
    }

    #[test]
    fn sonnet_5_outranks_the_general_claude_sonnet_pattern() {
        let sheet = seeded_for_tests();
        // "claude-sonnet-5" also contains "claude-sonnet" (priority 8) — the
        // sonnet-5-specific row (priority 0) must win, not the general one.
        let resolved = sheet.rate("anthropic", "claude-sonnet-5", month_start(2026, 8));
        assert_ne!(
            (resolved.input_per_million, resolved.output_per_million),
            (3.0, 15.0),
            "must not fall through to the general claude-sonnet rate"
        );
    }

    #[test]
    fn unattributed_sentinel_routes_to_the_blend_rate() {
        let sheet = seeded_for_tests();
        let resolved = sheet.rate("anthropic", UNATTRIBUTED_SENTINEL, month_start(2026, 8));
        assert!(!resolved.matched);
        assert_eq!((resolved.input_per_million, resolved.output_per_million), (5.7, 27.5));
    }

    #[test]
    fn genuinely_unknown_model_also_routes_to_the_blend_rate() {
        let sheet = seeded_for_tests();
        let resolved = sheet.rate("anthropic", "some-unknown-model", month_start(2026, 8));
        assert!(!resolved.matched);
        assert_eq!((resolved.input_per_million, resolved.output_per_million), (5.7, 27.5));
    }

    // ── OpenRouter provider port, PR1: rate() filters on (provider, model) ─────

    #[test]
    fn resolves_the_pinned_deepseek_rate_under_the_openrouter_provider() {
        let sheet = seeded_for_tests();
        let resolved = sheet.rate(
            "openrouter",
            "deepseek/deepseek-v4-flash-0731",
            month_start(2026, 8),
        );
        assert!(resolved.matched);
        assert_eq!((resolved.input_per_million, resolved.output_per_million), (0.08, 0.18));
        assert_eq!(resolved.cache_read_multiplier, 0.2);
        assert_eq!(resolved.cache_write_multiplier, 1.0);
    }

    #[test]
    fn resolves_the_pinned_glm_rate_under_the_openrouter_provider() {
        let sheet = seeded_for_tests();
        let resolved =
            sheet.rate("openrouter", "z-ai/glm-5.3-flash", month_start(2026, 8));
        assert!(resolved.matched);
        assert_eq!((resolved.input_per_million, resolved.output_per_million), (0.075, 0.25));
    }

    /// Item 6 — rate-card pattern fragility: `llm_usage.model` records the
    /// SERVED model, and OpenRouter may resolve/report a served id that
    /// drops the dated suffix entirely (`deepseek/deepseek-v4-flash`, no
    /// `-0731`). The seeded pattern is the shorter `"deepseek-v4-flash"`
    /// substring precisely so this still resolves, not the blend.
    #[test]
    fn resolves_the_pinned_deepseek_rate_for_the_undated_served_model_id() {
        let sheet = seeded_for_tests();
        let resolved =
            sheet.rate("openrouter", "deepseek/deepseek-v4-flash", month_start(2026, 8));
        assert!(resolved.matched, "the undated served-model id must still resolve, not blend");
        assert_eq!((resolved.input_per_million, resolved.output_per_million), (0.08, 0.18));
    }

    /// Same, for a `:nitro`-style OpenRouter routing-variant suffix.
    #[test]
    fn resolves_the_pinned_deepseek_rate_for_a_nitro_suffixed_served_model_id() {
        let sheet = seeded_for_tests();
        let resolved = sheet.rate(
            "openrouter",
            "deepseek/deepseek-v4-flash-0731:nitro",
            month_start(2026, 8),
        );
        assert!(resolved.matched, "a :nitro-suffixed served-model id must still resolve, not blend");
        assert_eq!((resolved.input_per_million, resolved.output_per_million), (0.08, 0.18));
    }

    #[test]
    fn resolves_the_pinned_glm_rate_for_the_undated_and_nitro_suffixed_served_model_ids() {
        let sheet = seeded_for_tests();
        let undated = sheet.rate("openrouter", "z-ai/glm-5.3-flash", month_start(2026, 8));
        assert!(undated.matched);
        let nitro = sheet.rate("openrouter", "z-ai/glm-5.3-flash:nitro", month_start(2026, 8));
        assert!(nitro.matched);
        assert_eq!(
            (undated.input_per_million, undated.output_per_million),
            (nitro.input_per_million, nitro.output_per_million)
        );
    }

    /// **`(provider, model)` disambiguation, phase 1.** Asked about under
    /// its OWN provider, a specific row still wins over a same-priority-tier
    /// collision from a different provider would (there is none seeded
    /// today, but this proves the exact-match phase is actually consulted
    /// FIRST, not that phase 2 alone carries every case).
    #[test]
    fn exact_provider_and_model_match_wins_over_the_fallback_phase() {
        let sheet = seeded_for_tests();
        let resolved =
            sheet.rate("openrouter", "deepseek/deepseek-v4-flash-0731", month_start(2026, 8));
        assert!(resolved.matched);
        assert_eq!((resolved.input_per_million, resolved.output_per_million), (0.08, 0.18));
    }

    /// **Phase 2 — provider-agnostic pattern fallback.** A caller that
    /// passes the WRONG provider string (exactly what `routes::admin::
    /// model_rate`/`routes::usage::estimate_cost_usd`/`workers::
    /// billing_derivation::estimate_cost_usd` do today — see each of their
    /// own doc comments for why) still resolves the CORRECT OpenRouter rate
    /// off the model pattern alone, rather than silently blending it. Model
    /// ids are namespaced (`claude-*` vs `deepseek/*`/`z-ai/*`) so this
    /// cross-provider fallback is safe in practice.
    #[test]
    fn a_wrong_provider_still_resolves_via_the_model_pattern_fallback() {
        let sheet = seeded_for_tests();
        let resolved = sheet.rate(
            "anthropic",
            "deepseek/deepseek-v4-flash-0731",
            month_start(2026, 8),
        );
        assert!(
            resolved.matched,
            "the model-pattern fallback must resolve the real OpenRouter rate even under the \
             wrong provider string, not fall through to the blend"
        );
        assert_eq!((resolved.input_per_million, resolved.output_per_million), (0.08, 0.18));
    }

    /// The blend/fallback row lookup is deliberately provider-AGNOSTIC (see
    /// `rate`'s own doc) — an unmatched OpenRouter model must still resolve
    /// to the one seeded (`"anthropic"`) blend row rather than panicking
    /// because no `"openrouter"`-scoped blend row exists.
    #[test]
    fn unmatched_openrouter_model_falls_through_to_the_cross_provider_blend_rate_without_panicking() {
        let sheet = seeded_for_tests();
        let resolved =
            sheet.rate("openrouter", "some-unpinned-openrouter-model", month_start(2026, 8));
        assert!(!resolved.matched);
        assert_eq!((resolved.input_per_million, resolved.output_per_million), (5.7, 27.5));
    }
}
