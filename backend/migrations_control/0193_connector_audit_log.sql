-- Migration: 0193_connector_audit_log
-- Description: PR 4.2 of the m365 enterprise connector plan (plan decision
--   #11, part 1) — the CONNECTOR compliance audit log: install/consent,
--   container routing, seat grant/revoke, ingestion-mode changes,
--   disconnect/purge, deletion propagation. actor + timestamp, append-only.
--
--   Sibling spec 4.3 (`query_audit_log`, migrations 0191/0069, open PR #697
--   at the time this migration was written) is a SEPARATE table — the
--   DISCLOSURE log, with a deliberate durable-WARN posture (a lost row is
--   an accepted residual: see that table's own header). THIS table is the
--   COMPLIANCE log and carries the HARD bar instead: an action this table
--   describes must not succeed silently unaudited. See
--   `services::connector_audit`'s module doc for the full durability
--   contract — `record`/`record_in_tx` propagate a write failure to the
--   caller (a request-path action whose audit row cannot be written FAILS
--   the action), while `record_or_queue` is the documented, retried,
--   escalate-to-`error!` fallback for worker/webhook paths that must not be
--   allowed to fail the underlying action they describe (e.g. mid-purge).
--
--   ── Digest, not payload ──
--   `subject` and `detail` are ids/counts ONLY — never a payload, a token,
--   or free-form content. `services::connector_audit` bounds both
--   defensively in Rust; the CHECK constraints below are the hard backstop.
--
--   ── Append-only ──
--   Only `account_isolation_select`/`account_isolation_insert` policies
--   exist below — no UPDATE/DELETE policy for the app role at all — the
--   same command-split precedent 0191/`query_audit_log` established:
--   append-only enforced by RLS's default-deny for an unmatched command,
--   not by mere convention.
--
--   ── The SELECT/RLS split ──
--   Same reasoning as 0191's header: RLS policies read Postgres session
--   GUCs, which cannot represent a Clerk org-role claim, so "org-admin
--   visible" is enforced at `routes::connector_audit::list_connector_audit`
--   via `services::deletion::is_admin_role`, BEFORE any row is read. SELECT
--   here is a plain account-scope backstop (defense in depth for the
--   account boundary only, not a claim about who is allowed to read).
--
--   ── actor_user_id nullable ──
--   Unlike `query_audit_log.asked_by_user_id` (always a real asking
--   human), a connector-audit row can be SYSTEM-caused (a seat reconciler
--   tick) or WEBHOOK-caused (an inbound provider event) with no human to
--   attribute — `actor_kind` records which, `actor_user_id` is NULL for
--   both. Bare uuid, no FK to `users(id)` — same "audit permanence must
--   survive the actor's own account departure" reasoning as
--   `membership_events.actor_user_id` (0125) and
--   `query_audit_log.asked_by_user_id` (0191).
--
--   ── Reserved migration numbers (announced) ──
--   This PR is ONLY permitted to stamp 0193 (control) / 0071 (tenant).
--   0191/0069 (open PR #697) and 0192/0070 (parallel 3.4 worktree) may be
--   ABSENT from this tree at review time — expected in-flight state, not a
--   gap this PR introduces or must fix.
--
-- Depends on: 0191_query_audit_log (RLS/route-split precedent, append-only
--             command-split convention this migration follows), 0125_
--             membership_events (nullable bare-uuid actor_user_id
--             precedent).
-- Down:
--   DROP POLICY IF EXISTS account_isolation_select ON connector_audit_log;
--   DROP POLICY IF EXISTS account_isolation_insert ON connector_audit_log;
--   DROP TABLE IF EXISTS connector_audit_log;

CREATE TABLE IF NOT EXISTS connector_audit_log (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id      UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    actor_user_id   UUID,
    actor_kind      TEXT        NOT NULL CHECK (actor_kind IN ('user', 'system', 'webhook')),
    action          TEXT        NOT NULL CHECK (action IN (
                        'install', 'consent', 'disconnect', 'purge',
                        'seat_grant', 'seat_revoke',
                        'route_container', 'unroute_container',
                        'ingestion_mode_change', 'deletion_propagated'
                    )),
    provider        TEXT,
    subject         TEXT        CHECK (subject IS NULL OR char_length(subject) <= 256),
    detail          JSONB       CHECK (detail IS NULL OR octet_length(detail::text) <= 4096),
    occurred_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- CodeRabbit round 1 on #701: `actor_kind`/`actor_user_id` pairing had no
    -- DB backstop — `('user', NULL)` and `('system'/'webhook', <uuid>)` were
    -- both insertable, so a write bypassing `services::connector_audit`'s
    -- `Actor` enum (the only thing enforcing this today) could misattribute
    -- or de-attribute a row. Mirrors the enum's own vocabulary exactly:
    -- `Actor::User(uid)` is the ONLY variant that carries a uuid.
    CONSTRAINT connector_audit_log_actor_pairing
        CHECK ((actor_kind = 'user') = (actor_user_id IS NOT NULL))
);

-- Admin route's read pattern: "the last N events for this account, newest
-- first" — `id` as an explicit tiebreaker so pagination is stable across a
-- burst of same-`occurred_at` rows (`routes::connector_audit::
-- list_connector_audit`'s `ORDER BY occurred_at DESC, id DESC`).
CREATE INDEX IF NOT EXISTS idx_connector_audit_log_account_occurred
    ON connector_audit_log (account_id, occurred_at DESC, id DESC);

-- ── RLS: account-scope backstop (SELECT), acting-session pairing (INSERT) ──
ALTER TABLE connector_audit_log ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation_select ON connector_audit_log;
DROP POLICY IF EXISTS account_isolation_insert ON connector_audit_log;
CREATE POLICY account_isolation_select ON connector_audit_log
    FOR SELECT
    USING (account_id = current_setting('app.account_id')::uuid);
CREATE POLICY account_isolation_insert ON connector_audit_log
    FOR INSERT
    WITH CHECK (
        account_id = current_setting('app.account_id')::uuid
        AND (
            actor_user_id IS NULL
            OR actor_user_id = NULLIF(current_setting('app.user_id', true), '')::uuid
        )
    );
