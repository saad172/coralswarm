-- Migration 0039: tenant on graph_subscriptions (CS-121 live + transcripts)
--
-- RSC (app-only) subscriptions — live channel/chat capture and the per-tenant
-- meeting-transcript subscription — are renewed with an app-only token for a
-- specific tenant, not a delegated user token. Record the tenant so the renewal
-- worker and lifecycle handler pick the right token. Empty = delegated (CS-117).

ALTER TABLE graph_subscriptions
    ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL DEFAULT '';
