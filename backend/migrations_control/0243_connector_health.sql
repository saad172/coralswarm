-- Migration: 0243_connector_health
-- Description: CS-446 (org-onboarding plan §3 item 16, D16,
--   `docs/plans/org-onboarding-from-the-invite.md`) — the durable record
--   behind D16's own build note:
--
--     "if the member whose token backs the directory leaves, the whole org
--      silently drops from rung 1 to rung 2 unless that raises a
--      `connector_health` warning."
--
--   ── This table is NOT the `connector_health` tracing field ──
--   Read this first, because the two share a name on purpose and are
--   different things. `connector_health = true` already exists in this
--   codebase as a TRACING FIELD on a handful of `warn!` lines
--   (`clerk_provision.rs`, `connectors/github_*.rs`, `bin/backfill_*.rs`),
--   required on every `graphs.is_empty()` fan-out branch by the CI grep-
--   guard `scripts/check_silent_drop_is_alertable.sh`. That marker makes a
--   condition visible to an OPERATOR TAILING LOGS. D16's sentence is about
--   an ADMIN, who does not read our logs and never will: a `warn!` is
--   exactly the "silently" that sentence objects to. So item 16 builds a
--   RECORD and a SURFACE. The tracing field keeps its meaning, its guard
--   and its call sites untouched; this table is the second, admin-facing
--   half, and the name is shared because it is the same subject.
--
--   ── What one row is ──
--   ONE ONGOING CONDITION, per account, per connector capability. Not one
--   observation: a sweep (or a read) that notices the same degradation
--   every 15 minutes must never accumulate rows, so idempotency is the
--   PRIMARY KEY `(account_id, provider, capability, condition)` and not
--   application logic — the same way `org_join_request_digests` (0240)
--   expresses "one digest per admin per day" as its own PK rather than as
--   a cooldown the worker has to keep re-deriving. Every writer is a bare
--   `INSERT ... ON CONFLICT (...) DO UPDATE SET last_observed_at = now()`.
--
--   `first_observed_at` / `last_observed_at` / `resolved_at` are the
--   lifecycle: the condition is CURRENT exactly when `resolved_at IS NULL`.
--   A condition that recurs after being resolved re-opens THE SAME ROW and
--   advances `first_observed_at` to now — the `CASE WHEN
--   connector_health.resolved_at IS NOT NULL THEN now() ELSE
--   connector_health.first_observed_at END` shape
--   `clerk_provision::remove_org_membership`'s own
--   `member_ocean_desired_state` upsert already uses for `offboarded_at`,
--   and for the identical reason: a NEW episode must not report a start
--   date from a PREVIOUS one, while a redundant re-observation of the SAME
--   episode must not move it either.
--
--   `condition` is part of the key rather than a payload column because two
--   different conditions degrading the same capability are two different
--   facts with two different remedies, and collapsing them would make the
--   second one overwrite the first's `first_observed_at` and its
--   `backing_*` context. "Is this capability degraded at all" is therefore
--   `EXISTS (... WHERE resolved_at IS NULL)` over the rows for that
--   `(provider, capability)`, not a single lookup — a deliberate trade.
--
--   ── The one condition this migration's CHECK admits, and why ──
--   `'backing_member_departed'` ONLY. D16 names exactly one trigger, and
--   the CHECK is deliberately narrow so the domain cannot drift by
--   accident: a writer for a new condition has to add the value here, in
--   its own migration, with its own reasoning.
--
--   Deliberately NOT modelled as this condition (see
--   `services::connector_health`'s own module doc for the full argument):
--     * a directory fetch that FAILED this minute — D16's "could not ask ≠
--       does not work here". `services::assertion_ladder::DirectorySignal::
--       Unavailable` already carries that, per person, per pass, and it is
--       a transient. Writing it here would turn the strongest rung's
--       best-known failure mode into a permanent-looking alarm.
--     * a revoked/expired grant, or a re-consent that dropped
--       `directory.readonly` — CS-431's `missing_scopes`/`ReconsentNotice`
--       already surfaces that, on the very page this feature's own notice
--       renders on, with the remedy (Reconnect) attached. The person still
--       works here; departure is the case where nobody can press that
--       button.
--
--   ── `backing_user_id` / `backing_email` ──
--   "Enough context for an admin to act" means, at minimum, WHOSE credential
--   the capability was resting on — an admin who is told "your directory
--   lookup stopped working" and not "it was Dana's Google grant, and Dana
--   left on Tuesday" cannot do anything with the notice. `ON DELETE SET
--   NULL` (not CASCADE): if the user row itself is ever deleted, the
--   degradation is still true and the record must survive its subject —
--   `backing_email`, captured at the moment of departure the same way
--   `org_membership_tombstones` captures an address at removal (0226,
--   D12/item 33), is what stays readable.
--
--   `detail` is a free-form JSONB bag on the same "never queried by value,
--   only rendered back to an admin" posture `org_join_requests.evidence`
--   and `slack_unmatched_email_notices`' observability columns take.
--
--   ── RLS: mirrors 0227/0233/0238/0240, not 0083's roster ──
--   Account-level operational state with no `graph_id`/ocean concept: a
--   bare account-scoped `account_isolation` policy covering every command.
--   Control-only, no tenant-tree twin — a connector-health record is
--   bookkeeping about an ORG and a CONNECTOR, never read back as content.
--   NOT on `scripts/check_rls_coverage.sh`'s AUTOMATIC 0083-derived roster
--   (that script reads `0083_consumer_rls.sql`'s own `CREATE POLICY` lines,
--   so a policy added by a LATER migration is never in it) — added to its
--   hand-curated `EXTRA_RLS_TABLES` list in this same PR, alongside
--   `org_join_requests`/`org_join_request_digests`/`org_domains`.
--   `coralswarm_app` (`rolbypassrls = f`) hard-errors an unscoped query
--   against this table's policy regardless of either roster.
--
--   THREE registration sites, not two (0238 and 0240 each missed the third
--   and left `consumer_rls_pg` red on `main` for a day): this PR adds
--   `connector_health` to (a) `EXTRA_RLS_TABLES` in
--   `scripts/check_rls_coverage.sh`, (b) `scripts/rls_coverage.manifest`,
--   and (c) `RLS_TABLES` in `backend/tests/consumer_rls_pg.rs`, whose step
--   5 asserts the live `pg_class.relrowsecurity` set EQUALS that roster —
--   no more, no less. Also added to `CONTROL_TABLES` in
--   `scripts/check_schema_parity.sh` in this same PR.
--
-- Depends on: 0001_init (accounts, users), 0083_consumer_rls (the
--             account_isolation-shaped policy convention),
--             0227_slack_unmatched_email_notices / 0233_slot_reservations /
--             0238_org_join_requests / 0240_org_join_request_digests (the
--             direct-account_id, control-only precedent this migration
--             follows).
-- Down:
--   DROP POLICY IF EXISTS account_isolation ON connector_health;
--   DROP TABLE IF EXISTS connector_health;

CREATE TABLE IF NOT EXISTS connector_health (
    account_id        UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    -- The `oauth_tokens.provider` literal, so a surface can key a notice to
    -- the same connector tile the member would act on ('google_chat').
    provider          TEXT        NOT NULL,
    -- WHICH capability of that connector degraded — a connector can be
    -- perfectly healthy for capture and still have lost one read
    -- ('domain_directory' is D16's rung 1).
    capability        TEXT        NOT NULL,
    condition         TEXT        NOT NULL CHECK (condition IN (
                                      'backing_member_departed'
                                  )),
    -- Whose credential the capability was resting on. NULLABLE because the
    -- user row may later be deleted (ON DELETE SET NULL) — the record
    -- outlives its subject, and `backing_email` is what stays readable.
    backing_user_id   UUID        REFERENCES users(id) ON DELETE SET NULL,
    backing_email     TEXT,
    detail            JSONB       NOT NULL DEFAULT '{}',
    first_observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_observed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- NULL = still current. This column IS the "is it still happening"
    -- answer; there is no separate boolean to disagree with it.
    resolved_at       TIMESTAMPTZ NULL,

    PRIMARY KEY (account_id, provider, capability, condition),

    -- A last observation before the first one, or a resolution before the
    -- condition started, are states this table cannot represent — the same
    -- posture `org_join_requests`' own paired-nullability CHECK takes, for
    -- the same reason: the DB refuses it rather than trusting every future
    -- writer to keep the timestamps ordered.
    CHECK (last_observed_at >= first_observed_at),
    CHECK (resolved_at IS NULL OR resolved_at >= first_observed_at)
);

-- The admin read (`GET /v1/orgs/connector-health`) — CURRENT degradations
-- for one org. Partial, because a resolved row is history and is never on
-- that path.
CREATE INDEX IF NOT EXISTS connector_health_open_by_account_idx
    ON connector_health (account_id, first_observed_at)
    WHERE resolved_at IS NULL;

ALTER TABLE connector_health ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON connector_health;
CREATE POLICY account_isolation ON connector_health
    USING (account_id = current_setting('app.account_id')::uuid)
    WITH CHECK (account_id = current_setting('app.account_id')::uuid);
