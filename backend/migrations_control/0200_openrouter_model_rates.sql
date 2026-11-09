-- Migration: 0200_openrouter_model_rates
-- Description: OpenRouter provider port (PR1) — seed `model_rates` fallback
--   rows for the two OpenRouter-routed models this PR wires up.
--
--   `backend/src/billing/rates.rs`'s `PriceSheet::rate` now filters on
--   `(provider, model)` (it always LOADED `provider`, per-row, but never
--   READ it until this PR) — these two rows are the first non-`'anthropic'`
--   rows the table has ever carried. See that module's doc comment for the
--   full ordered-substring-match / `as_of` contract; the short version: a
--   `model_rates` row only matches an `llm_usage` row whose OWN `provider`
--   column is an EXACT match (never substring) AND whose `model` column
--   CONTAINS this row's `model_pattern` (substring, not equality).
--
--   Provider-reported `cost_usd` (OpenRouter reports `usage.cost` on every
--   response, threaded straight into `llm_usage.cost_usd` by this PR) is
--   PREFERRED over these rows by `workers::billing_derivation::price_bucket`
--   whenever present — these rows are a FALLBACK (for the rare row where
--   the provider itself didn't report a cost, e.g. an errored/estimated
--   pending-row finalize) and a RECONCILIATION cross-check (`workers::
--   usage_reconciliation`), not the primary pricing path.
--
--   Pricing (pinned DeepInfra rates via OpenRouter, per this PR's own
--   description — the same allowlisted backend `llm::openai::
--   default_provider_allowlist` pins each model's `provider.order` to):
--
--     deepseek/deepseek-v4-flash-0731 — input $0.08/M, output $0.18/M,
--       cache read $0.016/M (cache_read_multiplier = 0.016/0.08 = 0.2),
--       cache write = input rate (cache_write_multiplier = 1.0 — unlike
--       Anthropic's explicit opt-in `cache_control` with a genuine write
--       premium, OpenRouter/DeepInfra's caching here is automatic with no
--       separate write cost).
--
--     z-ai/glm-5.3-flash — input $0.075/M, output $0.25/M, cache read
--       $0.015/M (cache_read_multiplier = 0.015/0.075 = 0.2, same ratio),
--       cache write = input rate (cache_write_multiplier = 1.0, same
--       reasoning as deepseek above).
--
--   `effective_from` is the fixed epoch marker `0148_model_rates.sql` already
--   established for every always-active row (2000-01-01, literal, never
--   `now()` — see that migration's header for why); `effective_to = NULL`.
--   No blend/fallback row is added here: an unmatched `('openrouter', ...)`
--   pair still safely falls through to the ONE existing cross-provider
--   blend row (`PriceSheet::rate`'s blend lookup is deliberately NOT
--   provider-scoped — see that function's own doc for why a provider-scoped
--   blend lookup would be a new panic risk for a provider with no seeded
--   blend row of its own).
--
--   `priority` only has to be unique/ordered WITHIN one provider's rows —
--   `rate()` filters by `provider` BEFORE ranking by `priority` — so 0/1
--   here don't collide with `0148`'s own 0-13 Anthropic priority scheme.
--
--   `model_pattern` is the SHORTEST stable substring, not the full request
--   id `llm::openai::default_provider_allowlist` keys on — `llm_usage.model`
--   records the SERVED model (`OpenAiResponse.model`/the SSE `model` field),
--   and OpenRouter may resolve/report back a served id that drops the dated
--   suffix (`deepseek/deepseek-v4-flash` instead of the requested
--   `deepseek/deepseek-v4-flash-0731`) or adds a routing suffix
--   (`z-ai/glm-5.3-flash:nitro`). `"deepseek-v4-flash"`/`"glm-5.3-flash"`
--   match all of those via the existing substring-containment semantics —
--   see `backend/src/billing/rates.rs`'s test module for the dated/undated/
--   `:nitro`-suffixed coverage.

INSERT INTO model_rates
    (provider, model_pattern, priority, input_per_million, output_per_million,
     cache_write_multiplier, cache_read_multiplier, effective_from, effective_to, is_blend_fallback)
VALUES
    ('openrouter', 'deepseek-v4-flash', 0, 0.08,  0.18, 1.0, 0.2, TIMESTAMPTZ '2000-01-01T00:00:00Z', NULL, false),
    ('openrouter', 'glm-5.3-flash',     1, 0.075, 0.25, 1.0, 0.2, TIMESTAMPTZ '2000-01-01T00:00:00Z', NULL, false);
