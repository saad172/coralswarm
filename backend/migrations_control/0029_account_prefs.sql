-- Migration: 0029_account_prefs
-- Description: Persist per-account preferences (ADR-0007 D11 / gap 4). The web
--              settings screen previously kept connector/LLM-mode toggles in
--              local component state and persisted nothing. Stored per active
--              account: for a personal account these are the user's prefs; for
--              an org account they are org-wide defaults (incl. the egress
--              policy that pins/forbids cloud vs local synthesis).
--
-- Depends on: 0001_init (accounts)
--
-- Down:
--   DROP TABLE IF EXISTS account_prefs;

CREATE TABLE IF NOT EXISTS account_prefs (
    account_id UUID        PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
    prefs      JSONB       NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
