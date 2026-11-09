-- Migration: 0270_graph_mail_routes
-- Description: Control-plane routing index for ORG-plane Outlook mail
--   change-notification subscriptions.
--
--   Graph notifications arrive at a public webhook knowing only a bare
--   `subscriptionId`. The webhook resolves it through the control pool
--   (`resolve_graph_subscription_account_id`, migration 0086). A PERSONAL
--   account's `graph_subscriptions` rows live on that same control/consumer
--   database, so that works. An ORG account's rows live on the org's own
--   tenant database (ADR-0007), which the webhook cannot search without
--   already knowing the org — so every org mail notification resolved to
--   "unknown subscription" and was silently dropped: per-thread "Keep saving
--   new replies" never captured anything on an org account.
--
--   This table is the missing pointer: subscription_id -> account_id. Only
--   the pointer lives here; client_state (the validation secret), resource,
--   expiry and owner stay on the tenant row, which the webhook reads after
--   hopping to the org's tenant pool. Same posture as the Teams doorbell
--   registry (0255): a routing index, not one account's content, so no RLS.
--
-- Depends on: 0269_google_meet_captures (numbering only).
-- Down:
--   DROP TABLE IF EXISTS graph_mail_routes;

CREATE TABLE graph_mail_routes (
    subscription_id TEXT        PRIMARY KEY,
    account_id      UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX graph_mail_routes_account_idx ON graph_mail_routes (account_id);
