-- Migration: 0257_org_install_capture_grace
-- Description: Phase G item 48 (founder, 2026-09-24) — one durable instant
--   for "when the connect-your-own emails began." The org-install capture
--   paths stay reachable for 30 days after this instant, then the gate in
--   `services::org_install_grace` refuses them. This is a clock, not a
--   feature flag computed from process start.
--
--   ── Which instant ──
--   The plan starts the 30 days when the connect emails begin, and that is
--   the earlier of item 49 and item 50. Item 49 (`member_connect`,
--   `workers::member_connect_nudge`) has been on main since 2026-09-21
--   (`991da2d2`). Item 50 merged as `aab538a1` at 2026-09-25 15:05:10 UTC.
--   A first `member_connect` send, if one exists in this database, is the
--   earlier candidate (`MIN(created_at)`, not `last_notified_at`, which
--   moves on every cooldown renewal). Item 50 has not been deployed long
--   enough to have its own sends, so its start is the merge instant, not a
--   notice row. `LEAST` of those two is the clock.
--
--   A live query of production was not available from the branch that wrote
--   this file (the Fly Postgres console is interactive). The MIN below is
--   computed when THIS migration applies, against the control plane it is
--   applied to, which is the database that holds the notices.
--
--   If that MIN is already more than 30 days old, using it would turn
--   capture off on the deploy that applies this file. Item 49's own merge
--   is 2026-09-21, so that cannot happen on the day item 50 merged, but a
--   delayed apply must not cut existing orgs off overnight. In that one
--   case the stamp is `now() - 29 days` (one day of grace left), not a
--   date invented to push the cutover out when the window is still open.
--   With no `member_connect` row at all, the stamp is the item 50 merge
--   exactly — never `now()`, so a later empty database still records
--   2026-09-25 15:05:10 UTC.
--
--   ── Control-only ──
--   Same category as `slack_unmatched_email_notices` (0227): a fleet-wide
--   fact about when emails began, no ocean, no tenant twin. The gate reads
--   it from the control pool. A tenant database does not have this table;
--   a missing relation fails open so an RSC install path whose only pool
--   is a tenant DB (the pre-split tests) keeps working until it has a
--   control pool to read.
--
--   One row. `id` is a boolean primary key constrained to true, the same
--   singleton shape `tenant_meta` uses, so a second insert cannot land.

CREATE TABLE org_install_capture_grace (
    id                       BOOLEAN     PRIMARY KEY DEFAULT TRUE CHECK (id),
    connect_emails_began_at  TIMESTAMPTZ NOT NULL
);

ALTER TABLE org_install_capture_grace ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS org_install_capture_grace_read ON org_install_capture_grace;
-- The row is a timestamp, not an account's content. The app role is not
-- the table owner, so it needs a policy; the owner (migrations, local
-- tests) bypasses RLS because this table is not FORCE'd.
CREATE POLICY org_install_capture_grace_read ON org_install_capture_grace
    FOR SELECT
    USING (true);

WITH bounds AS (
    SELECT
        (
            SELECT MIN(created_at)
            FROM slack_unmatched_email_notices
            WHERE provider = 'member_connect'
        ) AS first_member_connect,
        TIMESTAMPTZ '2026-09-25 15:05:10+00' AS item_50_merge
)
INSERT INTO org_install_capture_grace (id, connect_emails_began_at)
SELECT
    TRUE,
    CASE
        WHEN first_member_connect IS NOT NULL
         AND first_member_connect < now() - INTERVAL '30 days'
        THEN now() - INTERVAL '29 days'
        ELSE LEAST(
            COALESCE(first_member_connect, item_50_merge),
            item_50_merge
        )
    END
FROM bounds;
