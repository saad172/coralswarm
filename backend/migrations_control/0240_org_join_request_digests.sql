-- Migration: 0240_org_join_request_digests
-- Description: CS-438 (org-onboarding plan §3 item 5, D13,
--   `docs/plans/org-onboarding-from-the-invite.md`) — the admin-facing
--   notification for CS-437's join-request queue (0238_org_join_requests).
--
--   ── D13 — batched, one per admin per day, only on days with new
--   requests ──
--   D9, D11 and D12 all route people into approval, as does every org that
--   leaves auto-add off (the default) — their whole experience is "your
--   request has been sent to your admin." Before this PR there was NO
--   notification machinery of any kind: no digest, no inbox, no badge.
--   Without one, that sentence is not true and the queue is a silent
--   rejection — D13's own line: "If the notification is not built, 'sent
--   to your admin' should not be a sentence we show anyone."
--
--   Batched, because a busy workspace would otherwise flood an inbox with
--   one email per request. Only on days with changes, because an empty
--   digest trains people to ignore the full one — `workers::
--   org_join_request_digest` never sends when the newly-arrived count for
--   an admin is zero, whatever the total open queue looks like.
--
--   ── This table IS the idempotency, not a side effect of it ──
--   One row per `(account_id, admin_user_id, sent_on)` — the PRIMARY KEY.
--   The worker's claim step is a bare `INSERT ... ON CONFLICT (account_id,
--   admin_user_id, sent_on) DO NOTHING`: a second tick the same UTC day for
--   the same admin finds the row already there and sends nothing more,
--   without any separate lock or cooldown table (contrast
--   `slack_unmatched_email_notices`, whose cooldown is a renewable window
--   this table deliberately does NOT need — "already sent today" is a
--   simple date match, not a rolling interval).
--   `request_count` records how many NEW requests this specific digest
--   reported (the arrivals since this admin's own most recent digest
--   `sent_at`, across any earlier day) — it is what the worker's own
--   `MAX(sent_at)` lookup treats as "already told about", so a later day's
--   digest never re-reports a name this admin already saw. `CHECK
--   (request_count > 0)` — an empty digest is never written at all, so
--   there is nothing for that check to reject in practice; it documents
--   the invariant the worker's own `new_since` gate enforces rather than
--   trusting every future writer to keep re-deriving it.
--   `sent_on` is a UTC calendar date, deliberately — see
--   `workers::org_join_request_digest`'s own module doc for why "today" is
--   defined once, at the worker, rather than left to each caller's local
--   clock.
--
--   ── RLS: mirrors 0227/0233/0238, not 0083's roster ──
--   Account-level operational state with no `graph_id`/ocean concept, same
--   shape those three tables' own headers describe for themselves: a bare
--   account-scoped `account_isolation` policy covering every command.
--   Control-only, no tenant-tree twin — a digest send record is
--   bookkeeping about an ADMIN and an ORG, never read back as content. NOT
--   on `scripts/check_rls_coverage.sh`'s AUTOMATIC 0083-derived roster (a
--   policy added by a LATER migration is never in that automatic roster,
--   per that script's own doc comment) — added to its hand-curated
--   `EXTRA_RLS_TABLES` list in this same PR, alongside
--   `org_join_requests`'s own entry there. `coralswarm_app`
--   (`rolbypassrls = f`) hard-errors an unscoped query against this
--   table's policy regardless of either roster. Added to `CONTROL_TABLES`
--   in `scripts/check_schema_parity.sh` in this same PR.
--
-- Depends on: 0001_init (accounts, users), 0083_consumer_rls (the
--             account_isolation-shaped policy convention),
--             0227_slack_unmatched_email_notices / 0233_slot_reservations /
--             0238_org_join_requests (the direct-account_id, control-only
--             precedent this migration follows).
-- Down:
--   DROP POLICY IF EXISTS account_isolation ON org_join_request_digests;
--   DROP TABLE IF EXISTS org_join_request_digests;

CREATE TABLE IF NOT EXISTS org_join_request_digests (
    account_id     UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    admin_user_id  UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    sent_on        DATE        NOT NULL,
    request_count  INT         NOT NULL CHECK (request_count > 0),
    sent_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (account_id, admin_user_id, sent_on)
);

ALTER TABLE org_join_request_digests ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON org_join_request_digests;
CREATE POLICY account_isolation ON org_join_request_digests
    USING (account_id = current_setting('app.account_id')::uuid)
    WITH CHECK (account_id = current_setting('app.account_id')::uuid);
