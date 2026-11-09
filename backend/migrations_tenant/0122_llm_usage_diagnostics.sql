-- Safe typed metadata only; no prompts, responses, error bodies, or credentials.
-- Retained with existing usage records so incidents outlive ephemeral process logs.
ALTER TABLE llm_usage ADD COLUMN IF NOT EXISTS diagnostics JSONB;
