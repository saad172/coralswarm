-- Migration: 0349_deepseek_v41_flash_rate
-- Description: price `deepseek/deepseek-v4.1-flash`, standing source
--   extraction's new OpenRouter fallback (`SOURCE_EXTRACTION_FALLBACK_MODEL`,
--   default in `llm::model_config`). The managed gate prices every candidate
--   model before a send and refuses an unpriced one (`RefuseCode::UnpricedModel`),
--   so this row must exist before the fallback can be sent.
--
--   Control plane only: `model_rates` lives on control and the gate loads
--   its price sheet from the control pool for both content planes.
--
--   Rate: OpenRouter's DeepInfra fp8 card for this model on 2026-10-20,
--   input $0.11/M, output $0.34/M. Times the default 2.0
--   `MANAGED_PRICE_HEADROOM`, the window's `max_price` becomes 0.22/0.68,
--   which keeps the fp8 ZDR hosts for v4.1 eligible (DeepInfra, CoreWeave,
--   Morph). Cache multipliers follow 0200's OpenRouter rows.
--
--   Pattern `deepseek-v4.1-flash` cannot match `deepseek/deepseek-v4-flash`
--   or `deepseek/deepseek-v4-flash-0731` ("v4." vs "v4-"), and 0200's
--   `deepseek-v4-flash` pattern cannot match this model, so neither row
--   prices the other.

INSERT INTO model_rates
    (provider, model_pattern, priority, input_per_million, output_per_million,
     cache_write_multiplier, cache_read_multiplier, effective_from, effective_to, is_blend_fallback)
VALUES
    ('openrouter', 'deepseek-v4.1-flash', 2, 0.11, 0.34, 1.0, 0.2, TIMESTAMPTZ '2000-01-01T00:00:00Z', NULL, false);
