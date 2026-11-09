-- Migration: 0169_account_members_seat_tier
-- Description: billing self-serve launch PR B (5/5) — the member-facing half
--   of the seat-tier remodel (plan §1.4: "`account_members.seat_tier_id`
--   NOT NULL for team orgs — assigned at member-add time, picker defaults to
--   standard") plus §1.7's deferred-downgrade column
--   (`pending_seat_tier_id`).
--
--   ── The problem: "NOT NULL for team orgs" is a CROSS-TABLE condition ──
--   `account_members` (0001_init) has no notion of "team org" on itself —
--   that fact lives on `accounts.clerk_org_id` (0027: NULL = personal
--   account, NOT NULL = org account — see that migration's own header). A
--   plain `NOT NULL` on `account_members.seat_tier_id` would wrongly reject
--   every personal-account membership row (which must never carry a seat
--   tier — a solo `pro`/`free` account has no seats to price). A plain
--   CHECK on `account_members` alone cannot express "required, but only for
--   rows whose ACCOUNT has `clerk_org_id IS NOT NULL`" either, for the exact
--   reason `0156`'s header already gives for the same class of problem: "the
--   deciding fact... lives in ANOTHER TABLE, and Postgres CHECK constraints
--   may not reference one."
--
--   This is the SECOND time this exact shape has come up in this schema
--   (`0156`: grants must carry seats iff their plan is seat-priced — the
--   first instance; `0168`'s `grant_seat_tiers` looked like a third instance
--   in an earlier version of this train but was corrected to a plain
--   subquery-based RLS policy instead, once a security review pointed out
--   the denormalization there was unnecessary — see `0168`'s own header) and
--   it gets the SAME fix `0156` used: denormalize the deciding column onto
--   the table that needs it, keep the copy honest with a composite FK, then
--   state the real invariant as an ordinary same-table CHECK. Unlike `0168`'s
--   case, `account_members` genuinely cannot substitute a subquery-based
--   policy for this: this is a CHECK constraint, not an RLS `USING` policy,
--   and Postgres CHECK constraints (unlike RLS policies) truly cannot
--   reference another table's row at all — no subquery escape hatch exists
--   for this one. No trigger, no application-level assertion trusted alone
--   for the mirror itself — same reasoning `0156`'s header lays out in full;
--   not repeated here beyond this pointer.
--
--   ── The mechanism, applied here ──
--     1. `accounts.is_org_account BOOLEAN NOT NULL DEFAULT false`, kept in
--        agreement with `clerk_org_id` by a `BEFORE INSERT OR UPDATE`
--        trigger (`accounts_sync_is_org_account`) rather than by `GENERATED
--        ALWAYS ... STORED` — review-round fix; see "Why a plain trigger-
--        maintained column, not `GENERATED ALWAYS ... STORED`" below for
--        why the generated-column version of this migration was rejected.
--        The trigger recomputes `NEW.is_org_account := (NEW.clerk_org_id IS
--        NOT NULL)` on every INSERT/UPDATE of `accounts`, which is the same
--        "always in agreement by construction" guarantee a generated column
--        gives — nothing outside this trigger ever writes `is_org_account`
--        directly (nothing NEEDS to: `clerk_org_id` is the only fact that
--        determines it, and every writer of `clerk_org_id` already goes
--        through ordinary INSERT/UPDATE, which the trigger intercepts).
--     2. `accounts` gains `UNIQUE (id, is_org_account)` — the identical
--        "redundant against the primary key alone, exists only to be a
--        legal composite FK target" role `plans_id_is_seat_priced_key`
--        (0156) plays for `grants (plan_id, plan_is_seat_priced)`.
--     3. `account_members.account_is_org_account BOOLEAN NOT NULL` mirrors
--        it, held in agreement by `account_members_account_org_fk
--        (account_id, account_is_org_account) REFERENCES accounts
--        (id, is_org_account) DEFERRABLE INITIALLY IMMEDIATE` — see
--        "Why DEFERRABLE" below for the deferrability and its consequence.
--     4. `account_members_seat_tier_iff_org CHECK (account_is_org_account =
--        (seat_tier_id IS NOT NULL))` states the real invariant — the
--        `grants_seats_iff_seat_priced` shape, one migration family later.
--
--   ── Why a plain trigger-maintained column, not `GENERATED ALWAYS ...
--   ── STORED` (review-round fix) ──
--   An earlier version of this migration used `GENERATED ALWAYS AS
--   (clerk_org_id IS NOT NULL) STORED`. Postgres implements adding a STORED
--   generated column as a full TABLE REWRITE under `ACCESS EXCLUSIVE` —
--   unlike a plain `ADD COLUMN ... DEFAULT <constant>` (metadata-only on
--   PG11+, per 0164's own precedent on this identical table), a generated
--   column's value must be computed and stored for every existing row as
--   part of the `ALTER TABLE`, which cannot be metadata-only by definition.
--   `accounts` is the hottest table in this schema (read on effectively
--   every request path), and every other migration that has touched it
--   (`0027`'s `clerk_org_id`, `0164`'s `billing_enforcement_enabled`) is a
--   plain, metadata-only `ADD COLUMN ... DEFAULT` specifically to avoid this
--   class of lock — `SET LOCAL lock_timeout` only bounds how long this
--   migration WAITS to acquire the lock, not how long it HOLDS an
--   `ACCESS EXCLUSIVE` lock once acquired while rewriting the table, so the
--   generated-column version broke that precedent for the exact table it
--   matters most on. The plain-column-plus-trigger version restores it: the
--   `ADD COLUMN IF NOT EXISTS ... BOOLEAN NOT NULL DEFAULT false` below is
--   metadata-only, and the backfill that follows is an ordinary `UPDATE`
--   (still a real write, still worth noting on a large table, but a
--   ROW-level write under normal MVCC locking, not a table rewrite under
--   `ACCESS EXCLUSIVE`) — the same shape 0164's own header already uses for
--   its `billing_enforcement_enabled` column and backfill script. (This
--   distinction matters far more against a production-sized `accounts` table
--   than against the pg16 test container this migration is also verified
--   against — the point is prod safety, and this migration is written for
--   that case even though nothing in the CI-sized container would surface
--   the difference.)
--
--   ── Why `accounts` gets a real column here, when 0162's header argued
--   ── AGAINST widening `accounts` for Stripe linkage ──
--   That argument (0162) was about ADDING STATE to the hottest table in the
--   schema for a fact only ONE caller (a webhook) ever writes and only two
--   ever read — third-party billing state in the way of every unrelated
--   query. `is_org_account` is different in kind, not just degree: it is a
--   trigger-maintained, deterministic projection of a column `accounts`
--   already carries (`clerk_org_id`, added by 0027, read on effectively
--   every request path already) — nothing NEW is written to `accounts` by
--   any caller this migration or any future one introduces (every writer of
--   `clerk_org_id` already goes through the ordinary INSERT/UPDATE path the
--   trigger intercepts; no call site gains a new column to remember to set),
--   so there is no new write-CALLER to keep in sync and no risk of the two
--   columns disagreeing. It is closer to an index than to freely-writable
--   state — the trigger is what makes that true despite not being a
--   generated column anymore.
--
--   ── Why `DEFERRABLE INITIALLY IMMEDIATE` on `account_members_account_org_fk`
--   ── (review-round fix — was non-deferrable) ──
--   A non-deferrable composite FK checks on EACH statement, which makes a
--   personal↔org conversion of an account that already has member rows
--   permanently impossible once this constraint exists: flipping
--   `accounts.is_org_account` (via `clerk_org_id`) changes the value every
--   existing `account_members` row's mirror must agree with, but the two
--   tables cannot be updated in the same statement, and neither update order
--   satisfies a non-deferrable FK mid-transaction (the account update alone
--   leaves every member row's stale mirror disagreeing with the new
--   `accounts` row; the member update alone has nothing yet to agree with).
--   `DEFERRABLE INITIALLY IMMEDIATE` keeps the constraint checked
--   immediately for the OVERWHELMING common case (a single-statement INSERT/
--   UPDATE, which is everything this schema does today) while allowing a
--   caller who explicitly needs the conversion to `SET CONSTRAINTS
--   account_members_account_org_fk DEFERRED` for one transaction, then: (1)
--   update the `accounts` row (flips `is_org_account` via the trigger), (2)
--   update every affected `account_members` row's `account_is_org_account`
--   (and `seat_tier_id`, to satisfy `account_members_seat_tier_iff_org`, in
--   the same transaction) to match, and (3) COMMIT — the FK (and the other
--   two CHECK constraints, which are always checked per-statement/per-row
--   but are satisfied once both writes above have happened) validates once,
--   at commit, against the final state rather than the illegal intermediate
--   one. This is a REGISTERED CONSEQUENCE of this migration, not a
--   currently-implemented feature: no route or worker in this tree converts
--   an account between personal and org today, so nothing exercises this
--   path yet, but the schema no longer forecloses it outright the way a
--   non-deferrable FK would.
--
--   ── Why NOT extend this same trick to enforce "personal accounts must
--   ── NOT carry a seat tier" as a separate rule ──
--   It doesn't need a separate rule: `account_members_seat_tier_iff_org` is
--   already an IFF, not a one-directional "org implies seat tier" — the
--   same CHECK that requires a tier for an org member also forbids one for
--   a personal member (`account_is_org_account = false` forces
--   `seat_tier_id IS NULL`), exactly mirroring how `grants_seats_iff_
--   seat_priced` is one CHECK that rejects BOTH the seat-priced-without-
--   seats case and the flat-plan-with-seats case (0156's own header calls
--   this out explicitly as "the mirror-image error").
--
--   ── `pending_seat_tier_id` (plan §1.7: deferred downgrades) ──
--   Always nullable, no mirrored invariant of its own beyond
--   `account_members_pending_requires_current_tier CHECK
--   (pending_seat_tier_id IS NULL OR seat_tier_id IS NOT NULL)` — a pending
--   downgrade only makes sense for a member who currently HAS a tier to
--   downgrade FROM (necessarily an org member, transitively, via the CHECK
--   above). This migration does not implement the downgrade-application
--   logic (PR E: "applied to `granted_credits` at period roll" per plan
--   §1.7) — it only makes the column exist and structurally sound to write
--   into. No FK-mirror needed here: `pending_seat_tier_id` references
--   `seat_tiers` directly (same as `seat_tier_id`), and there is no
--   cross-table fact about IT that needs protecting beyond "the member
--   already has a tier", which is a same-table CHECK once `seat_tier_id`
--   exists on this same row.
--
--   ── Why this migration ALSO adds a BEFORE INSERT trigger — deliberately
--   ── departing from `0156`'s "this tree uses none" stance ──
--   `account_members` is written from FOUR separate, pre-existing Rust call
--   sites this PR is expressly forbidden from touching (review-round fix —
--   an earlier version of this migration said THREE; a fourth, uncounted
--   raw `INSERT INTO account_members` was found by grepping the tree for
--   every occurrence, not just the ones named in the handoff doc):
--     1. `clerk_provision::ensure_personal_account` (`clerk_provision.rs`
--        ~line 434) — personal-account provisioning. Uncounted in the
--        original header; its member row is always `account_is_org_account
--        = false` (a personal account's `is_org_account` is always false),
--        so it is unaffected by the seat-tier default arm but still needed
--        the mirror column filled in.
--     2. `clerk_provision::ensure_org_membership` (`clerk_provision.rs`
--        ~line 671).
--     3. `clerk_provision::refresh_org_membership_from_claims`
--        (`clerk_provision.rs` ~line 944).
--     4. `Store::insert_account_member` (`backend/src/store/mod.rs` ~line
--        15942) — the actual `INSERT` `routes::teams::invite_member` calls
--        THROUGH; the original header named `teams::invite_member` itself as
--        the call site, which is the route handler, not the function that
--        issues the INSERT — corrected here to name the real writer.
--   None of the four know these two new columns exist (see the handoff doc's
--   "Invariants from PR A" and this PR's own "no route changes" scope).
--   `grants` (0156) is different in exactly the way that matters: it has ONE
--   writer (`billing::issuance::issue_grant`), so 0156 could require every
--   caller to pass `plan_is_seat_priced` explicitly without touching more
--   than that one function. Requiring the same of `account_members` would
--   mean either editing four route/worker/store call sites in a migration PR
--   that has explicitly promised not to (breaking the stated scope), or
--   shipping a migration that makes every EXISTING, UNMODIFIED insert into
--   `account_members` start throwing `null value in column
--   "account_is_org_account" violates not-null constraint` the moment this
--   migration is applied to a live database — a production outage on every
--   personal signup, org join, and invite, caused by a schema-only PR.
--
--   A BEFORE INSERT trigger closes that gap by filling in both new columns
--   from the referenced account WHENEVER a caller (old or new) leaves them
--   NULL, which is exactly what "assigned at member-add time... default
--   standard" (plan §1.4) already describes in words — this trigger is that
--   sentence, expressed as an INSERT-time default a plain column DEFAULT
--   cannot express (a `DEFAULT` expression may not reference another column
--   of the same row, and `account_is_org_account`/`seat_tier_id` both need
--   to read `NEW.account_id`). It does NOT weaken the invariant: a caller
--   that DOES supply a value (a future PR's explicit tier picker, plan
--   §1.4's "picker defaults to standard" implies an operator CAN override
--   the default) is left untouched, and `account_members_seat_tier_iff_org`/
--   `account_members_account_org_fk` below still reject a wrong or
--   mismatched explicit value exactly as they would with no trigger at all
--   — the trigger only ever fills a gap, never overrides or validates. This
--   is also not unprecedented ON THIS TABLE specifically: `account_members`
--   already carries `account_members_block_revoked` (0082, AFTER INSERT
--   DEFERRABLE), so a second, differently-shaped trigger here is one more
--   instance of a pattern this table already uses, not a new category of
--   procedural code being introduced into a tree that avoids it — 0156's
--   "this tree uses none" was true of `grants`, not of `account_members`.
--
--   ── Backfill: existing team-org members get the TIER THEIR ACCOUNT IS
--   ── ACTUALLY ENTITLED TO, not `standard` unconditionally (review-round
--   ── fix — the original version of this migration stamped every team-org
--   ── member `standard`, which is WRONG for any account whose live grant is
--   ── `team_premium`) ──
--   Every pre-existing `account_members` row for a team org
--   (`clerk_org_id IS NOT NULL`) predates this column and holds no seat-tier
--   decision anybody actually made — there is no historical fact to recover
--   (unlike `0156`'s pre-flight guard, which refused to GUESS a seat count
--   because guessing wrong would silently misstate a real purchase). But the
--   original header's claim that "nothing in this schema has ever priced a
--   MEMBER individually before this migration" — offered as the reason
--   `standard` was a safe uniform default — is WRONG: `billing::renewal`'s
--   `CURRENT_GRANT_SQL` (`backend/src/billing/renewal.rs`) already entitles
--   every member of an account with an active `team_premium` grant to the
--   PER-SEAT `team_premium` allowance (15,000 credits) at the next period
--   roll, today, before this migration ever runs. Stamping such a member
--   `standard` (3,000 credits) would silently understate their real,
--   already-registered entitlement the moment PR E starts reading
--   `seat_tier_id` instead of the account-level grant — a real regression,
--   not a neutral placeholder.
--
--   The correct backfill instead maps each team-org member's `seat_tier_id`
--   from THEIR ACCOUNT's currently active grant, mirroring
--   `CURRENT_GRANT_SQL`'s own selection semantics exactly (unrevoked,
--   newest by `granted_at`: `revoked_at IS NULL ORDER BY granted_at DESC
--   LIMIT 1`) so the backfilled tier agrees with what the account is ALREADY
--   entitled to, not a fresh guess:
--     * active grant's plan is `team_premium`  -> `premium`
--     * anything else (`team_standard`, any other plan, or NO active grant
--       at all) -> `standard`
--   An account with no active grant at all still needs a VALID (not NULL)
--   tier for `account_members_seat_tier_iff_org` to hold, and `standard` is
--   the correct floor for it the same way it always was — this migration
--   does not change what a grant-less account's members are backfilled to,
--   only what an actively `team_premium`-granted account's members are.
--   `bin`/route work in a later PR (not schema) is free to re-tier any
--   specific member for real; this migration's job is only to leave every
--   row in a VALID state that agrees with the account's existing
--   entitlement, which `account_members_seat_tier_iff_org` now requires.
--
--   Backfill order matters: `account_is_org_account` (a plain, backfillable
--   boolean) must be populated and made NOT NULL BEFORE
--   `account_members_seat_tier_iff_org` is added, and `seat_tier_id` must be
--   backfilled for every row where that column reads `true` before the same
--   CHECK is added — otherwise the ALTER TABLE ... ADD CONSTRAINT that
--   introduces it fails outright against the very rows it needs to describe
--   (Postgres validates a new CHECK against ALL existing rows unless told
--   not to, and this migration does not use NOT VALID — the whole point is
--   a hard guarantee that holds for every row from the moment the
--   constraint exists, not a promise to validate later). Both new columns
--   (`account_is_org_account` and `seat_tier_id`) are therefore added BEFORE
--   the single backfill pass below runs, rather than seat_tier_id being
--   added afterward in a second wave — review-round fix, collapsing what was
--   previously two full-table `UPDATE`s over `account_members` into one:
--   the account-org mirror and the seat-tier mapping are computed from the
--   same `accounts`/`grants` join, so there is no reason to scan the table
--   twice to write them.
--
-- CONTROL-PLANE ONLY: `account_members` already has no tenant twin (0001,
--   listed in `scripts/check_schema_parity.sh`'s `CONTROL_TABLES` since that
--   script's introduction) and carries no RLS policy of its own (see
--   `0164`'s header: "`accounts` carries no RLS policy... same category as
--   `dek_wrapped`/`clerk_org_id`" — `account_members` is the same category,
--   confirmed against `backend/tests/consumer_rls_pg.rs`'s `RLS_TABLES`
--   roster, which does not list it). This migration adds columns to an
--   already-registered, already-RLS-free control-only table; no schema-
--   parity or RLS-coverage roster needs a new entry for `account_members`
--   itself. `seat_tiers` (0165) needs no new roster entry either — it was
--   already added to `CONTROL_TABLES` in that migration's own PR.
--
-- Depends on: 0001_init.sql (`account_members`), 0027_clerk_identity.sql
--   (`accounts.clerk_org_id`, the source fact `is_org_account` mirrors),
--   0156_grants_seats.sql (the composite-FK-mirror + IFF-CHECK technique
--   this migration applies a third time), 0165_seat_tiers.sql
--   (`seat_tiers`, the FK target for `seat_tier_id`/`pending_seat_tier_id`).
-- Down:
--   DROP TRIGGER IF EXISTS account_members_default_seat_tier_trg ON account_members;
--   DROP FUNCTION IF EXISTS account_members_default_seat_tier();
--   ALTER TABLE account_members DROP CONSTRAINT IF EXISTS account_members_pending_requires_current_tier;
--   ALTER TABLE account_members DROP CONSTRAINT IF EXISTS account_members_seat_tier_iff_org;
--   ALTER TABLE account_members DROP CONSTRAINT IF EXISTS account_members_account_org_fk;
--   ALTER TABLE account_members DROP COLUMN IF EXISTS pending_seat_tier_id;
--   ALTER TABLE account_members DROP COLUMN IF EXISTS seat_tier_id;
--   ALTER TABLE account_members DROP COLUMN IF EXISTS account_is_org_account;
--   ALTER TABLE accounts DROP CONSTRAINT IF EXISTS accounts_id_is_org_account_key;
--   DROP TRIGGER IF EXISTS accounts_sync_is_org_account_trg ON accounts;
--   DROP FUNCTION IF EXISTS accounts_sync_is_org_account();
--   ALTER TABLE accounts DROP COLUMN IF EXISTS is_org_account;

SET LOCAL lock_timeout = '3s';

-- ── 1. The source-of-truth column on `accounts`, trigger-maintained ────────
-- Plain, metadata-only ADD COLUMN with a constant DEFAULT (PG11+) — NOT a
-- STORED generated column. See header "Why a plain trigger-maintained
-- column, not GENERATED ALWAYS ... STORED" for why: a generated column here
-- would force a full table rewrite under ACCESS EXCLUSIVE on the hottest
-- table in the schema.
ALTER TABLE accounts
    ADD COLUMN IF NOT EXISTS is_org_account BOOLEAN NOT NULL DEFAULT false;

-- Backfill: a real UPDATE (row-level writes, not a table rewrite) for every
-- account whose `clerk_org_id` disagrees with the DEFAULT the ADD COLUMN
-- above just gave every existing row. Guarded by `is_org_account = false`
-- so this is safe to re-run.
UPDATE accounts
   SET is_org_account = true
 WHERE clerk_org_id IS NOT NULL
   AND is_org_account = false;

-- Keeps `is_org_account` in agreement with `clerk_org_id` for every future
-- INSERT/UPDATE, the same "always in agreement by construction" guarantee a
-- generated column gives, without the rewrite-on-ADD-COLUMN cost.
CREATE OR REPLACE FUNCTION accounts_sync_is_org_account()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.is_org_account := (NEW.clerk_org_id IS NOT NULL);
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS accounts_sync_is_org_account_trg ON accounts;
CREATE TRIGGER accounts_sync_is_org_account_trg
    BEFORE INSERT OR UPDATE ON accounts
    FOR EACH ROW
    EXECUTE FUNCTION accounts_sync_is_org_account();

-- Idempotency posture for ADD CONSTRAINT (review-round fix — this migration
-- previously mixed guarded `ADD COLUMN IF NOT EXISTS` with bare
-- `ADD CONSTRAINT`; every `ADD CONSTRAINT` below is now guarded the same
-- way, consistently). No existing migration in this tree (checked 0155,
-- 0163, 0164 as directed, plus every other `ADD CONSTRAINT` in
-- `migrations_control`) guards a constraint with a `pg_constraint` check
-- either — this migration establishes that guard rather than following a
-- precedent that does not exist, so the whole file is idempotent front to
-- back rather than half so.
--
-- ── CodeRabbit round 1, finding 1: CONCURRENTLY-then-attach was
-- ── consciously NOT used for `accounts_id_is_org_account_key` ──
-- `ADD CONSTRAINT ... UNIQUE (id, is_org_account)` below builds its backing
-- unique index under `ACCESS EXCLUSIVE` for the whole build, same as any
-- other plain `ADD CONSTRAINT ... UNIQUE` — no `CONCURRENTLY`-then-`ALTER
-- TABLE ... ADD CONSTRAINT ... UNIQUE USING INDEX` two-step, which would
-- avoid that lock but at the cost of two separate DDL statements outside a
-- single transaction (a real, if narrow, window where the index exists but
-- the constraint does not). That tradeoff only pays for itself against a
-- large table. Prod `accounts` held ~6 rows total, verified 2026-08-19
-- during PR A's seed-member-allowance backfill (`#652`) — at that size the
-- `ACCESS EXCLUSIVE` window this statement holds is indistinguishable from
-- instant, and `CONCURRENTLY`-then-attach would add real mechanical
-- complexity for no measurable benefit. Kept in the plain form
-- deliberately, and recorded here so this question is not reopened next
-- review round without an actual row-count change to justify revisiting it.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'accounts_id_is_org_account_key'
    ) THEN
        ALTER TABLE accounts
            ADD CONSTRAINT accounts_id_is_org_account_key UNIQUE (id, is_org_account);
    END IF;
END $$;

-- ── 2. The mirror + seat-tier columns on `account_members` ─────────────────
-- All three new columns are added BEFORE the backfill runs (review-round
-- fix — collapses two full-table UPDATEs into one; see header).
ALTER TABLE account_members
    ADD COLUMN IF NOT EXISTS account_is_org_account BOOLEAN;

ALTER TABLE account_members
    ADD COLUMN IF NOT EXISTS seat_tier_id UUID REFERENCES seat_tiers(id);

ALTER TABLE account_members
    ADD COLUMN IF NOT EXISTS pending_seat_tier_id UUID REFERENCES seat_tiers(id);

-- ── 3. Single-pass backfill (review-round fix, findings 1 + collapsed
-- double-UPDATE) ────────────────────────────────────────────────────────────
-- Maps each team-org member's `seat_tier_id` from the ACCOUNT's currently
-- active (unrevoked, newest) grant's plan, mirroring `billing::renewal`'s
-- `CURRENT_GRANT_SQL` selection semantics exactly (`revoked_at IS NULL
-- ORDER BY granted_at DESC LIMIT 1`) — `team_premium` maps to `premium`,
-- everything else (including no active grant at all) maps to `standard`.
-- See header "Backfill: existing team-org members get the TIER THEIR
-- ACCOUNT IS ACTUALLY ENTITLED TO" for why unconditional `standard` was
-- wrong. Sets `account_is_org_account` in the same pass for every row still
-- missing it (personal-account rows too, where it always resolves false and
-- `seat_tier_id` is left NULL).
--
-- ── Pre-flight (CodeRabbit round 1, finding 2) ──
-- The backfill's own lookup below resolves each team-org member's
-- `seat_tier_id` by CODE ('standard'/'premium') — the exact same lookup
-- shape `account_members_default_seat_tier()` (below, §5) uses for its own
-- single-code default, and the SAME finding applies here for the SAME
-- reason: an unfiltered `WHERE code = ...` can silently resolve to a
-- deactivated row (or NULL, if the row is somehow gone), and either way the
-- backfill's UPDATE would stamp `seat_tier_id = NULL` for every row it
-- touches — which then fails, potentially thousands of rows and one
-- ALTER TABLE later, at `account_members_seat_tier_iff_org`'s CHECK
-- violation, an error that names nothing about `seat_tiers` at all. This
-- guard fails loudly, up front, naming the real cause instead.
DO $$
DECLARE
    v_standard_active BOOLEAN;
    v_premium_active BOOLEAN;
BEGIN
    SELECT EXISTS (
        SELECT 1 FROM seat_tiers WHERE code = 'standard' AND is_active = true
    ) INTO v_standard_active;
    SELECT EXISTS (
        SELECT 1 FROM seat_tiers WHERE code = 'premium' AND is_active = true
    ) INTO v_premium_active;
    IF NOT v_standard_active OR NOT v_premium_active THEN
        RAISE EXCEPTION 'migration 0169 backfill: seat_tiers must carry an ACTIVE ''standard'' and an ACTIVE ''premium'' row (0165''s seed) before this backfill can run — found standard active=%, premium active=%. This is a seat_tiers catalog precondition, not an account_members bug: fix the seat_tiers rows first, then re-run this migration.',
            v_standard_active, v_premium_active;
    END IF;
END $$;

WITH active_grant AS (
    SELECT DISTINCT ON (g.account_id) g.account_id, p.code AS plan_code
      FROM grants g
      JOIN plans p ON p.id = g.plan_id
     WHERE g.revoked_at IS NULL
     ORDER BY g.account_id, g.granted_at DESC
)
UPDATE account_members m
   SET account_is_org_account = a.is_org_account,
       seat_tier_id = CASE
           WHEN a.is_org_account THEN (
               SELECT id FROM seat_tiers
                WHERE code = CASE WHEN ag.plan_code = 'team_premium'
                                   THEN 'premium' ELSE 'standard' END
                  AND is_active = true
           )
           ELSE NULL
       END
  FROM accounts a
  LEFT JOIN active_grant ag ON ag.account_id = a.id
 WHERE a.id = m.account_id
   AND (m.account_is_org_account IS NULL
        OR (a.is_org_account AND m.seat_tier_id IS NULL));

ALTER TABLE account_members
    ALTER COLUMN account_is_org_account SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'account_members_account_org_fk'
    ) THEN
        -- DEFERRABLE INITIALLY IMMEDIATE — see header "Why DEFERRABLE" for
        -- the personal<->org conversion path this unblocks.
        ALTER TABLE account_members
            ADD CONSTRAINT account_members_account_org_fk
                FOREIGN KEY (account_id, account_is_org_account)
                REFERENCES accounts (id, is_org_account)
                DEFERRABLE INITIALLY IMMEDIATE;
    END IF;
END $$;

-- ── 4. The real invariants, now expressible ────────────────────────────────
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'account_members_seat_tier_iff_org'
    ) THEN
        ALTER TABLE account_members
            ADD CONSTRAINT account_members_seat_tier_iff_org
                CHECK (account_is_org_account = (seat_tier_id IS NOT NULL));
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'account_members_pending_requires_current_tier'
    ) THEN
        ALTER TABLE account_members
            ADD CONSTRAINT account_members_pending_requires_current_tier
                CHECK (pending_seat_tier_id IS NULL OR seat_tier_id IS NOT NULL);
    END IF;
END $$;

-- ── 5. BEFORE INSERT default-filler — see header for why this exists ──────
-- Fills `account_is_org_account`/`seat_tier_id` from the referenced account
-- ONLY when the caller left them NULL, so every EXISTING, unmodified
-- `INSERT INTO account_members (account_id, user_id, role) VALUES (...)`
-- call site keeps working unchanged after this migration, while a future
-- caller that supplies either column explicitly is left untouched (and is
-- still checked by the constraints above).
--
-- SUNSET (finding 6): this default arm is NOT meant to default a billing
-- fact forever. PR F (seat lifecycle) must make every call site that adds a
-- team-org member pass `seat_tier_id` explicitly (the real tier picker plan
-- §1.4 describes); once that lands, the `IF ... seat_tier_id IS NULL`
-- branch below should become `RAISE EXCEPTION` for an org member with no
-- explicit tier, the same way `0156`'s callers are required to pass
-- `plan_is_seat_priced` with no silent default at all. Until then, the
-- `RAISE WARNING` below is the visibility floor: every time the default arm
-- actually fires for an org member, it is logged, so silent defaulting is
-- at least observable rather than invisible between now and PR F.
CREATE OR REPLACE FUNCTION account_members_default_seat_tier()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_is_org_account BOOLEAN;
    v_standard_tier_id UUID;
BEGIN
    IF NEW.account_is_org_account IS NULL THEN
        SELECT is_org_account INTO v_is_org_account
          FROM accounts WHERE id = NEW.account_id;
        -- `v_is_org_account` is NULL only if `NEW.account_id` does not (yet)
        -- reference an existing account. Left as-is (no COALESCE — review-
        -- round fix: the earlier COALESCE-to-false branch was dead code,
        -- because either way the row is rejected: NULL here trips the
        -- NOT NULL constraint on `account_is_org_account` below, and the
        -- pre-existing `account_id` FK independently rejects the row for
        -- the real reason (no such account) once constraints are checked —
        -- coercing to `false` first changed nothing about the outcome, only
        -- which error fired first).
        NEW.account_is_org_account := v_is_org_account;
    END IF;

    IF NEW.account_is_org_account AND NEW.seat_tier_id IS NULL THEN
        -- `is_active = true` (finding 5 — the original lookup had no active
        -- filter and no missing-row guard) and a loud failure if the row is
        -- somehow gone, rather than silently inserting a NULL tier that the
        -- CHECK constraint below would reject with a much less specific
        -- error.
        SELECT id INTO v_standard_tier_id
          FROM seat_tiers WHERE code = 'standard' AND is_active = true;
        IF v_standard_tier_id IS NULL THEN
            RAISE EXCEPTION 'account_members_default_seat_tier: no ACTIVE seat_tiers row with code ''standard'' found — cannot default account_id=%, user_id=%''s seat tier',
                NEW.account_id, NEW.user_id;
        END IF;
        -- Sunset visibility (finding 6) — see the function's own header
        -- comment above for the PR F plan this WARNING exists to make
        -- concrete in the meantime.
        RAISE WARNING 'account_members_default_seat_tier: defaulting account_id=%, user_id=% to the standard seat tier — no explicit tier was supplied by the caller. Scheduled for removal in PR F (seat lifecycle): once every call site passes a tier explicitly, this branch becomes RAISE EXCEPTION.',
            NEW.account_id, NEW.user_id;
        NEW.seat_tier_id := v_standard_tier_id;
    END IF;

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS account_members_default_seat_tier_trg ON account_members;
CREATE TRIGGER account_members_default_seat_tier_trg
    BEFORE INSERT ON account_members
    FOR EACH ROW
    EXECUTE FUNCTION account_members_default_seat_tier();
