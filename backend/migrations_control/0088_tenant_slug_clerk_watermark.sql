-- Migration: 0088_tenant_slug_clerk_watermark
-- Description: Ordering watermark for Clerk-driven slug reconciliation.
--
--   `tenant_databases.slug` MUST equal the Clerk organization slug: the web app
--   derives the tenant host from `useOrganization().organization.slug` (it has
--   no alternative source — `/v1/me` sits behind the binding gate itself), and
--   the gate resolves the routing row from that Host. Onboarding asserts the
--   two agree (`orgt_verify_clerk_slug`), but a RENAME in Clerk afterwards
--   re-opens the identical breakage on a live tenant. `organization.updated`
--   (routes/clerk_webhook.rs) now reconciles the rename into this table.
--
--   Clerk does NOT guarantee webhook ordering, and Svix redelivers. Without a
--   watermark, a delayed `organization.updated` carrying an OLDER slug would
--   clobber a newer one and re-break routing — the exact failure this
--   reconciliation exists to prevent. `clerk_slug_updated_at` records the Clerk
--   `data.updated_at` of the last event APPLIED to this row, and
--   `tenancy::routing::reconcile_slug` applies an event only when it is
--   strictly newer, making the reconciliation last-writer-wins by CLERK's clock
--   rather than by arrival order.
--
--   Deliberately NOT `tenant_databases.updated_at`: that column moves for
--   unrelated reasons (e.g. `set_schema_version` after a tenant migration), so
--   comparing a Clerk event timestamp against it would skip valid renames.
--
--   NULL means "no Clerk event has been applied yet" — the state every row
--   onboarded before this migration is in — and the first event always applies.
--   Additive and nullable, so no backfill and no table rewrite.
ALTER TABLE tenant_databases
    ADD COLUMN clerk_slug_updated_at TIMESTAMPTZ;

COMMENT ON COLUMN tenant_databases.clerk_slug_updated_at IS
    'Clerk data.updated_at of the last organization.updated event applied to this row; '
    'the ordering watermark that makes slug reconciliation safe against out-of-order '
    'and replayed webhook delivery. NULL = no event applied yet.';

-- ── OPS-4 privilege: the app may now rewrite THESE TWO COLUMNS, nothing else ──
--
-- Migration 0083 revoked `INSERT, UPDATE, DELETE ON tenant_databases` from
-- `coralswarm_app` on the explicit, documented premise that "the live
-- app-request path only ever READS it ... no `routes/` file writes this table".
-- The `organization.updated` handler makes that premise false: it is a live
-- request-path writer, so once OPS-4 switches the app's connection role this
-- reconciliation would fail with SQLSTATE 42501 on every rename — silently, in
-- the sense that the only symptom is the broken-routing bug it exists to fix,
-- reappearing.
--
-- The fix is a COLUMN-scoped grant, not a table-scoped one, so 0083's actual
-- security property survives intact. What 0083 was protecting against is spelled
-- out in its own comment: "an app-layer compromise with write access here could
-- repoint an org's database". That attack needs `db_secret_ref` (or
-- `account_id`/`clerk_org_id`/`status`), and none of them are granted here —
-- they stay as unwritable to `coralswarm_app` as they were the moment before
-- this migration. Nor is INSERT or DELETE: the app still cannot create or
-- destroy a routing row, only rewrite the public subdomain label of one that
-- already exists, which is precisely (and only) what reconciling a Clerk rename
-- requires.
--
-- `updated_at` is included because the reconciling UPDATE stamps it, same as
-- every other writer of this table.
--
-- Column-level UPDATE also needs SELECT on the columns read in the WHERE clause;
-- `coralswarm_app` retains table-level SELECT from 0083, so that is covered.
--
-- Unconditional: 0083 creates `coralswarm_app` (and asserts its attributes) on
-- every run, so the role is guaranteed to exist by the time this migration runs.
GRANT UPDATE (slug, clerk_slug_updated_at, updated_at)
    ON tenant_databases TO coralswarm_app;
