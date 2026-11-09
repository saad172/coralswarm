-- Migration: 0238_org_join_requests
-- Description: CS-437 (org-onboarding plan §3 items 4 + 14,
--   `docs/plans/org-onboarding-from-the-invite.md`, D3/D9/D10/D19) — the
--   join-request queue every non-admin-initiated path into an org routes
--   through: D9's reservation loser, D11's non-matching-domain auto-add
--   refusal, D12's tombstoned returning member, and D19's matched-but-not-
--   member roster hit all land here, subject to the SAME approve/reject flow.
--   Only `source = 'self_request'` has a route in THIS PR (`POST
--   /v1/join-requests`) — the other four sources are pre-declared for
--   items 9/29/38 to write into without a follow-up migration.
--
--   ── D3 — a real user waiting, not a member; `pending` already means
--   something else ──
--   The person this table describes is a real, signed-in CoralSwarm user
--   with their own personal ocean the entire time they wait — they are
--   simply not a member of the org they asked to join. That is the OPPOSITE
--   of what `org_invites::derive_pending` (`org_invites.rs:303`) calls
--   "pending": an admin-invited Clerk user who HAS a membership (and has
--   ALREADY consumed a billing seat) but has not yet signed in. Rendering
--   both states as "pending" would leave an admin unable to tell who is
--   already costing money from who is asking to. This table exists so the
--   two states never share a name or a row shape.
--
--   Approval is NOT a local write alone — D3's own sequence is Clerk-first
--   (reserve a slot, call Clerk's Admin API `create_org_membership`, mirror
--   locally with `clerk_provision::ensure_org_membership`, unwind both the
--   Clerk membership and the reservation on any failure) — see
--   `routes::org_invites::provision_clerk_membership`/`mirror_local_membership`
--   (CS-437) and `routes::org_join_requests::approve`. A `decision =
--   'approved'` row is
--   written ONLY after that sequence has actually produced a real Clerk
--   membership; this table is never the source of truth for who is a
--   member, `account_members` still is.
--
--   ── D9 — `source = 'pool_full'` ──
--   The loser of a `billing::slot_pool::reserve_slot` race (item 9, not
--   built in this PR) falls back to approval SILENTLY to the requester —
--   "your request has been sent to your admin," never "the org's seat pool
--   is full." `evidence.pool_full` on the eventual APPROVE decision (set by
--   this PR's own `approve` handler when `reserve_slot` returns `None`) is
--   the admin-facing signal that Stripe was charged for this approval;
--   `source` itself stays whatever originally queued the request (most
--   often `self_request`) — `pool_full` as a `source` value is reserved for
--   item 9's own auto-add-refused-by-capacity case, a DIFFERENT moment (at
--   enqueue, not at decision) than the evidence flag this PR writes.
--
--   ── D10 — a rejected row is the durable, per-org suppression, never
--   deleted ──
--   The person is never told they were declined, is never re-invited BY
--   THAT ORG, and other orgs are unaffected — rejection is per-org by
--   nature (D4's send-cooldown is per-PERSON; folding the two together was
--   flagged as a near-mistake in D10's own text). Durable rather than
--   deleted, or she requests again next week and the admin re-decides
--   forever. The partial unique index below (`WHERE decision IS NULL`)
--   is what makes a rejected row inert going forward without ever removing
--   it: a second `self_request` for the same `(account_id, user_id)` finds
--   the row COUNT for "open requests" unaffected (the old row already has a
--   `decision`), so nothing blocks a fresh queue attempt at the DB layer —
--   `services::org_join_requests::enqueue_join_request` is what reads the
--   most recent DECIDED row and returns `Suppressed` instead of inserting a
--   new one, keeping the suppression logic in Rust rather than a second
--   unique index that would have to special-case "the newest rejected row."
--
--   ── D19 — `source = 'matched_not_member'` ──
--   A person who already has a CoralSwarm account (a personal one, say) and
--   is findable on an org's roster (Slack/Teams/Chat) but is not a member
--   of that org resolves as "Matched" today, so the invite pass (which
--   targets Unmatched addresses) never fires for them, and fan-out drops
--   them too (`account_members`-intersected). Item 29 (not built in this
--   PR) queues a "join this org" email into this SAME table with this
--   source value, subject to the identical cooldown/tombstone/gate rules
--   every other source gets — their existing account is kept; the request
--   is for MEMBERSHIP, not for an account.
--
--   `source = 'unverified_domain'` (item 9/37/38) and
--   `source = 'tombstoned_returning'` (D12, item 3/33 — this PR's own
--   requester route DOES write this one: `POST /v1/join-requests` checks
--   `clerk_provision::invite_suppressed_by_tombstone` and records the
--   source when the requester was previously removed from this same org)
--   round out the five-value domain items 9/29/38 all route into.
--
--   ── Shape ──
--   One open (`decision IS NULL`) row per `(account_id, user_id)` — the
--   partial unique index below IS the "AlreadyOpen" outcome
--   `services::org_join_requests::enqueue_join_request` reads off a unique-
--   violation. `evidence` is a free-form JSONB bag (the verified email used,
--   whether a tombstone hit, `pool_full`/`tier` on the decision row) rather
--   than a wide sparse column set — nothing in it is ever queried by value,
--   only rendered back to an admin or logged, the same posture
--   `slack_unmatched_email_notices`' own `workspace_id`/`channel_id`
--   "observability only" columns take for their own non-keyed context.
--   `CHECK ((decided_at IS NULL) = (decision IS NULL))` is the same paired-
--   nullability invariant `slot_reservations`' own
--   `CHECK ((released_at IS NULL) = (release_reason IS NULL))` enforces for
--   the identical reason: a decided-but-timestampless (or timestamped-but-
--   undecided) row is never a state this table can represent, so the DB
--   itself refuses it rather than trusting every writer to keep the two in
--   sync.
--
--   ── RLS: mirrors 0227/0233, not 0083's roster ──
--   Account-level operational state with no `graph_id`/ocean concept — same
--   shape 0227's and 0233's own headers describe for their own tables: a
--   bare account-scoped `account_isolation` policy covering every command.
--   Control-only, no tenant-tree twin — a join request is bookkeeping about
--   a PERSON and an ORG, never read back as content. NOT on
--   `scripts/check_rls_coverage.sh`'s AUTOMATIC 0083-derived roster (that
--   script only reads `backend/migrations_control/0083_consumer_rls.sql`'s
--   own `CREATE POLICY` lines — a policy added by a LATER migration, like
--   this one, is never in that automatic roster) — same as
--   `slot_reservations`/`slack_unmatched_email_notices`/`grants`/
--   `grant_seat_tiers`. CodeRabbit round 1 (CS-437 PR #1021) caught that
--   this table's own production readers/writers (`services::
--   org_join_requests`) shipped unscoped in the first cut despite that gap —
--   fixed by opening `tenancy::scope::begin_account_scoped` in every one of
--   them, AND by adding `org_join_requests` to `check_rls_coverage.sh`'s
--   hand-curated `EXTRA_RLS_TABLES` list (the same permanent-fix pattern
--   `slack_unmatched_email_notices` already uses), so this table's coverage
--   is now checked mechanically going forward, not merely documented here.
--   `coralswarm_app` (`rolbypassrls = f`) hard-errors an unscoped query
--   against this table's policy regardless of either roster. Added to
--   `CONTROL_TABLES` in `scripts/check_schema_parity.sh` in this same PR.
--
-- Depends on: 0001_init (accounts, users), 0083_consumer_rls (the
--             account_isolation-shaped policy convention),
--             0227_slack_unmatched_email_notices /
--             0233_slot_reservations (the direct-account_id, control-only
--             precedent this migration follows).
-- Down:
--   DROP POLICY IF EXISTS account_isolation ON org_join_requests;
--   DROP TABLE IF EXISTS org_join_requests;

CREATE TABLE IF NOT EXISTS org_join_requests (
    id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id    UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    user_id       UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    source        TEXT        NOT NULL CHECK (source IN (
                                  'self_request',
                                  'pool_full',
                                  'unverified_domain',
                                  'tombstoned_returning',
                                  'matched_not_member'
                              )),
    evidence      JSONB       NOT NULL DEFAULT '{}',
    requested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    decided_at    TIMESTAMPTZ NULL,
    decided_by    UUID        REFERENCES users(id),
    decision      TEXT        CHECK (decision IN ('approved', 'rejected')),

    CHECK ((decided_at IS NULL) = (decision IS NULL))
);

-- One open request per person per org (D10/D3) — also the signal
-- `services::org_join_requests::enqueue_join_request` reads a unique-
-- violation off of to return `EnqueueOutcome::AlreadyOpen` instead of a
-- second row.
CREATE UNIQUE INDEX IF NOT EXISTS org_join_requests_one_open_per_member_idx
    ON org_join_requests (account_id, user_id)
    WHERE decision IS NULL;

-- The admin queue read (`GET /v1/orgs/join-requests`) — open rows for one
-- org, oldest first.
CREATE INDEX IF NOT EXISTS org_join_requests_open_by_account_idx
    ON org_join_requests (account_id, requested_at)
    WHERE decision IS NULL;

ALTER TABLE org_join_requests ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON org_join_requests;
CREATE POLICY account_isolation ON org_join_requests
    USING (account_id = current_setting('app.account_id')::uuid)
    WITH CHECK (account_id = current_setting('app.account_id')::uuid);
