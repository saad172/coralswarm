-- Migration: 0104_connector_identity_verified_link
-- Description: C5 (verified-email identity mapping), PHASE ONE ONLY —
--              infrastructure + the matcher. NOT wired into any routing path:
--              `resolve_org_fanout_destinations` (connectors/slack.rs) and
--              `resolve_clicking_user` (routes/slack_interactivity.rs) are
--              untouched by this migration and its accompanying code. See
--              `connectors::slack::resolve_verified_slack_identity`'s doc
--              comment and docs/connectors/slack.md §1's C5 row for what
--              phase two still has to do.
--
-- `connector_identities` (migration 0049) stores
-- (provider, workspace_id, external_user_id) -> canonical_name ONLY — there
-- is no column anywhere saying "this Slack member IS this CoralSwarm user."
-- Adds a nullable verified link plus provenance:
--
--   user_id           the matched `users.id`. NULL is the common, LEGITIMATE
--                      case — a Slack member who has never signed up for
--                      CoralSwarm, or an org guest/contractor — and
--                      `canonical_name` alone still serves display for that
--                      row. NULL is not an error state.
--   link_method        how the link was established. Exactly one method
--                      exists today ('verified_email' — see the matcher);
--                      TEXT + CHECK rather than an enum so a second method
--                      (e.g. a future admin override) doesn't need a type
--                      migration, only a widened CHECK.
--   link_verified_at   when the match was confirmed.
--
-- The provenance CHECK ties all three together: either all three are NULL
-- (no link established) or all three are set (a confirmed link) — never a
-- `user_id` with no provenance, never provenance with no `user_id`.
--
-- `ON DELETE SET NULL`: a CoralSwarm user being deleted must not cascade into
-- deleting connector-directory reference data — the row's `canonical_name` is
-- still valid display data for the Slack member independent of whether their
-- CoralSwarm account still exists.
--
-- ── Why only `users.email`, no secondary-email table ──────────────────────
-- An earlier draft of this phase planned a `user_emails` table (Clerk
-- supports multiple verified addresses per user). Founder correction
-- (2026-07-27): C5 email matching serves the ORG plane only — an org Slack
-- workspace is provisioned with company addresses, and the org's CoralSwarm
-- account is invited with the same ones, so comparing against `users.email`
-- alone is sufficient (domains match by construction). The personal plane
-- never needed this at all: the connecting member's Slack identity is
-- already proven at OAuth time (`oauth_tokens.provider_user_id`), strictly
-- stronger than an email inference. `user_emails` was therefore dropped from
-- this phase entirely — do not reintroduce it without a fresh design reason.
--
-- ── Paired tenant migration ────────────────────────────────────────────────
-- migrations_tenant/0015 adds the IDENTICAL three columns as a BARE (no-FK)
-- `user_id` — `users` is control-plane-only (org-tenancy split put it there
-- exclusively), so there is nothing on the tenant plane to reference; same
-- treatment as every other cross-plane column already on this table's
-- neighbors (see migrations_tenant/0001_tenant_baseline.sql's header: "Cross-
-- DB references ... are bare uuid columns with no FK"). check_schema_parity.sh
-- CASCADE-drops `users` from a scratch control-tree DB before diffing, which
-- strips this exact FK down to a bare column first — so the two migrations
-- are expected to differ ONLY in that FK clause.
--
-- ── RLS: none added, and none needed ───────────────────────────────────────
-- `connector_identities` is deliberately OFF both RLS rosters (0083's header:
-- "genuinely shared reference data (id -> display name), not per-account
-- content" — two different accounts can legitimately install the OAuth app
-- into the very same external Slack workspace and share a row). Adding a
-- nullable pointer to AT MOST one CoralSwarm user does not change that: the
-- row is still keyed by (provider, workspace_id, external_user_id), not by
-- account, and still holds no secret. `scripts/check_schema_parity.sh`'s
-- CONTROL_ONLY_RLS_TABLES / TENANT_ONLY_RLS_TABLES lists neither list this
-- table, and neither needs to after this change.
--
-- Depends on: 0001_init (users), 0049_connector_identity_self_reef
--             (connector_identities)
-- Down:
--   ALTER TABLE connector_identities
--       DROP CONSTRAINT IF EXISTS connector_identities_link_provenance_chk,
--       DROP CONSTRAINT IF EXISTS connector_identities_link_method_chk,
--       DROP COLUMN IF EXISTS link_verified_at,
--       DROP COLUMN IF EXISTS link_method,
--       DROP COLUMN IF EXISTS user_id;

ALTER TABLE connector_identities
    ADD COLUMN IF NOT EXISTS user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS link_method TEXT,
    ADD COLUMN IF NOT EXISTS link_verified_at TIMESTAMPTZ,
    ADD CONSTRAINT connector_identities_link_method_chk
        CHECK (link_method IS NULL OR link_method IN ('verified_email')),
    ADD CONSTRAINT connector_identities_link_provenance_chk
        CHECK (
            (user_id IS NULL AND link_method IS NULL AND link_verified_at IS NULL)
            OR
            (user_id IS NOT NULL AND link_method IS NOT NULL AND link_verified_at IS NOT NULL)
        );
