-- Migration: 0178_org_connector_flags
-- Description: the per-org connector flag registry (Foundation train PR 1.3,
--   `.scratch/m365-enterprise/plan.md`). Today `ORG_SLACK_OAUTH_ENABLED`
--   (`routes/oauth.rs::org_slack_oauth_enabled`) and `ORG_TEAMS_ENABLED`
--   (`workers/member_ocean_reconciler.rs::org_teams_enabled`) are
--   process-global env vars — flipping either turns the feature on for
--   EVERY org account fleet-wide, all-or-nothing. Piloting a connector on a
--   single org (the M365 plan's decision #10, "prerequisite for piloting
--   M365 on one org") is not possible with that shape.
--
--   This table adds the missing per-org override dimension. Resolution
--   semantics (implemented in `connector_flags::resolve`): a row for
--   `(clerk_org_id, provider)` wins outright; when no row exists, the
--   caller's own env-var reader (`org_slack_oauth_enabled()` /
--   `org_teams_enabled()`) supplies the default, UNCHANGED from today's
--   behavior. So creating this table and shipping zero rows is byte-
--   identical to before this migration — every org keeps reading the same
--   global env default it always has.
--
--   ── Column choices ──
--   `clerk_org_id` — `TEXT NOT NULL`, non-empty (`CHECK`), no FK: the same
--   bare-text convention this repo already uses for a Clerk org id on
--   `accounts`/`org_membership_tombstones`/`org_revocations`/
--   `member_ocean_desired_state`/`connector_installs` (migrations
--   0027/0081/0082/0090/0092/0121) — Clerk owns that id's lifecycle, so a
--   local FK would be a false promise of referential integrity Clerk itself
--   does not offer. Deliberately NOT `account_id`: the two existing flags
--   this table replaces are both org-identity gates checked against
--   `tenant.org_context()?.org_id` (a Clerk org id), never against a
--   CoralSwarm `account_id` — see `routes/oauth.rs::reject_org_oauth_management`,
--   `routes/slack.rs::reject_org_slack_management`, and
--   `routes/teams.rs::reject_org_named_teams`, all of which extract exactly
--   `ctx.org_id` and nothing else from the `Tenant`. Keying on `clerk_org_id`
--   also means an org can be flagged before or without ever completing
--   CoralSwarm account provisioning ordering games.
--
--   `provider` — `TEXT NOT NULL`, non-empty (`CHECK`). Free text, not an
--   enum: this table gates TWO different flags that are not both OAuth
--   connectors in the narrow sense (`'slack'` maps to `ORG_SLACK_OAUTH_ENABLED`,
--   `'teams'` maps to `ORG_TEAMS_ENABLED`, gating the named-team routes, not
--   an OAuth flow) — an enum would need widening for every future flag this
--   registry picks up (Microsoft, per the M365 plan's Train 2/3). Same
--   "free TEXT key, no enum" choice `connector_installs.provider` already
--   made for the identical reason.
--
--   `enabled` — `BOOLEAN NOT NULL`, no default: a row only ever exists
--   because an operator explicitly set it (see the admin setter,
--   `routes::admin::set_connector_flag`); there is no "half-created" state
--   to default away.
--
--   `created_at` / `updated_at` — standard pair; `updated_at` is bumped by
--   the setter's `ON CONFLICT ... DO UPDATE`, `created_at` is not (mirrors
--   `connector_installs.created_at`'s "first install timestamp" precedent).
--
--   ── Primary key + uniqueness ──
--   Surrogate `id UUID PRIMARY KEY DEFAULT gen_random_uuid()` (every non-
--   ledger table in this repo carries one — see `connector_installs`'s own
--   header for the full REPLICA IDENTITY rationale), plus a single plain
--   `UNIQUE (clerk_org_id, provider)` index: unlike `connector_installs`,
--   there is no nullable dimension here splitting the uniqueness regime, so
--   one ordinary unique index (not two partial ones) is sufficient and is
--   also the setter's `ON CONFLICT` target.
--
--   ── No RLS, deliberately ──
--   Same class as `connector_installs` (0121) and `accounts.
--   billing_enforcement_enabled` (0164): this table holds no secrets and no
--   account-scoped content, only "is provider X on for org Y" routing
--   config, and it is read at a point in the request (the org gate, before
--   any account-scoped transaction is opened) where `SET LOCAL
--   app.account_id` may not even be available yet. Adding RLS here would
--   need the identical `SECURITY DEFINER` bootstrap carve-out 0084/0086/0121
--   already document at length for the same reason, for no additional
--   safety — this table cannot be queried in a way that leaks anything
--   beyond "which orgs have an override for which provider", which only an
--   operator (X-Internal-Token) can ever write in the first place.
--
-- Depends on: none (bare table, no FKs).
-- Down:
--   DROP INDEX IF EXISTS uq_org_connector_flags_org_provider;
--   DROP TABLE IF EXISTS org_connector_flags;

CREATE TABLE IF NOT EXISTS org_connector_flags (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    clerk_org_id TEXT        NOT NULL CHECK (clerk_org_id <> ''),
    provider     TEXT        NOT NULL CHECK (provider <> ''),
    enabled      BOOLEAN     NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One override row per (org, provider); also the setter's ON CONFLICT target.
CREATE UNIQUE INDEX IF NOT EXISTS uq_org_connector_flags_org_provider
    ON org_connector_flags (clerk_org_id, provider);
