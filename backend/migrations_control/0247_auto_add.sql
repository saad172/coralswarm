-- Migration: 0247_auto_add
-- Description: CS-447 (org-onboarding plan §3 item 9,
--   `docs/plans/org-onboarding-from-the-invite.md`, D1/D11/D12/D13/D16) —
--   auto-add: the state behind "automatically add colleagues with an
--   @acme.com address", gated on domain + pool + tombstone.
--
--   This is the feature D1 exists to keep away from the customer's credit
--   card. Everything below is shaped by that one sentence: the toggle is
--   OFF unless a row says otherwise, the tier it spends against is written
--   down rather than inferred, the slot it consumes is one an admin already
--   bought, and every add it performs is recorded so an admin is told it
--   happened.
--
--   Four independent pieces, one file because they ship as one decision:
--
--     (1) `org_domains` — a domain may be verified by ONE org, globally.
--     (2) `org_auto_add_settings` — the toggle, and the tier it spends.
--     (3) `org_auto_adds` — the ledger of who was added without a human.
--     (4) `org_join_request_digests.auto_add_count` — D13's second section.
--
--   ── (1) A domain may be verified by ONE org, globally ─────────────────
--
--   **Founder decision, 2026-09-22, amending D11** (the second 2026-09-22
--   callout at the top of D11). This REVERSES a choice 0242's own header
--   argues for at length, so the reversal is recorded here rather than
--   left to be discovered as a contradiction: 0242 chose
--   `UNIQUE (account_id, domain)` and wrote "deliberately NOT `domain`
--   alone", on the grounds that a parent and a subsidiary (or an agency and
--   its client) genuinely can both control one domain and a global index
--   would let whoever claimed first permanently deny the second. 0242 also
--   said, in as many words, that "D11 does not settle this; it is called
--   out in CS-443's PR body rather than decided silently here."
--
--   It is settled now, the other way, and the reason is exactly the thing
--   that changed between 0242 and this migration: **auto-add became real.**
--   While nothing read these rows to spend money, two verified copies of
--   `acme.com` were inert bookkeeping. With item 9 live, an address at a
--   doubly-verified domain matches BOTH orgs, and the system would be
--   choosing which org a stranger silently joins WHILE CONSUMING A PAID
--   SLOT. There is no correct answer for it to choose, and the wrong one
--   costs an org money and leaks a person into a workspace neither admin
--   put them in.
--
--   **First org to verify wins.** The cost is accepted and named by the
--   founder: a first claimant can deny a legitimate parent/subsidiary or
--   agency/client, and unpicking that needs support.
--
--   **`WHERE verified_at IS NOT NULL` is the whole design.** Claiming stays
--   per-account — two orgs may both CLAIM `acme.com`, both mint their own
--   `verification_token`, and both publish their own TXT record; only the
--   first to actually PROVE it takes the domain. A full `UNIQUE (domain)`
--   would have made the claim itself the race, which is a land-grab any
--   signed-up account could run against every domain it can spell, with no
--   proof of anything. A partial index means the only thing that is
--   exclusive is the only thing that was ever evidence.
--
--   The refusal surfaces as a 409 an admin can act on ("already verified by
--   another organization") — `services::org_domains::mark_verified` maps
--   this index's own unique violation (SQLSTATE 23505, constraint
--   `org_domains_verified_domain_global_idx`) to
--   `VerifyOutcome::TakenByAnotherOrg`. Reading the other org's row to
--   produce that message is IMPOSSIBLE under 0242's `account_isolation`
--   policy and must stay that way — which is why the refusal is derived
--   from the write's own error and not from a pre-read. That also makes it
--   race-free: two orgs verifying the same domain in the same millisecond
--   resolve in the index, not in a check-then-act window.
--
--   **No backfill, verified rather than assumed.** Production control plane
--   was read on 2026-09-22 at `_sqlx_migrations.max(version) = 241` with
--   `to_regclass('public.org_domains') IS NULL` — 0242 merged the same day
--   but has never been deployed, so the table does not exist in production
--   and cannot hold a row that violates this index. 0242 and 0247 will be
--   applied in the same `release.sh` run, creating the table empty and
--   indexing it empty.
--
--   ── (2) `org_auto_add_settings` — off by default, and the tier ────────
--
--   **D11: "The toggle itself is off by default — the founder's framing: a
--   setting an admin turns on to manage spend."** `enabled BOOLEAN NOT NULL
--   DEFAULT false` is half of that; the other half is that NO ROW is also
--   off (`services::org_auto_add::read_settings` returns a disabled
--   settings value for a missing row rather than an `Option` every caller
--   would have to remember to treat as "off"). A fresh org has no row, and
--   therefore auto-adds nobody, without anything having been run for it.
--
--   `default_seat_tier_id` — **founder decision, 2026-09-22: the admin
--   picks the tier on the toggle.** Approval lets an admin choose the slot
--   tier per request (item 40, CS-442, migration 0241). Auto-add has no
--   human at the moment of spending, so the CHOICE has to have been made
--   earlier, and this column is where it is made. NULL means `standard`,
--   the identical NULL semantics `slot_reservations.seat_tier_id` carries
--   (0241's own header) — one convention for "no tier recorded means the
--   default tier", not two.
--
--   **The pool check is against THAT TIER ONLY.** If the chosen tier has no
--   free slot, auto-add falls to approval with `source = 'pool_full'` even
--   when another tier has slots sitting idle. That is deliberate and is a
--   negative test: a `premium` toggle must never quietly spend a
--   `standard` slot an admin bought for someone else. The read is
--   `billing::slot_pool::read_slot_pool_for_tier_ledger` (through
--   `reserve_slot`), never the report reader — CS-444's own finding: a
--   reduce/consume writes `grant_seat_tiers`, so a capacity gate must read
--   the lagging key the ledger is written under or a member mid-downgrade
--   makes a slot look free.
--
--   `REFERENCES seat_tiers(id)` with the default NO ACTION (not `SET
--   NULL`): silently demoting a `premium` toggle to `standard` because a
--   tier row was deleted is a spend decision nobody made. A tier in use by
--   a live toggle cannot be deleted, which is the honest outcome.
--
--   `account_id` as the PRIMARY KEY, no surrogate `id`: exactly one
--   settings row per org, enforced by the key itself rather than by a
--   unique index over a surrogate — the same shape
--   `org_join_request_digests` (0240) uses for its own natural key.
--
--   ── (3) `org_auto_adds` — the ledger D13's amendment needs ────────────
--
--   **An auto-added member never creates a join request.** That is the
--   whole point of the feature and it is also the gap: `org_join_requests`
--   is where every other way into an org leaves a trace, and auto-add
--   leaves none. Before this table, an admin's only evidence that auto-add
--   had spent a slot was noticing a new name in the members list.
--
--   Deliberately NOT a row in `org_join_requests` with some
--   `source = 'auto_added'`:
--     * that table's own `CHECK (source IN (...))` (0238) enumerates five
--       REQUEST sources, and an auto-add is not a request — writing one
--       would make "who asked to join" a query that has to know to exclude
--       a value;
--     * `org_join_requests_one_open_per_member_idx` would then have to be
--       reasoned about for rows that are born decided;
--     * D13's amendment asks for a **second, separate section** in the
--       digest, "visually distinct: one asks for a decision, the other
--       reports a fact". Two sections reading one table filtered by a magic
--       source value is how those two facts get confused later.
--
--   `rung` — **which rung admitted them**, D13's amendment in as many
--   words ("who was added automatically since the last one, and which rung
--   they qualified on"). `CHECK (rung IN ('directory', 'member_and_domain'))`
--   is the D16 ladder's top TWO rungs and nothing else: rung 3
--   (`single_signal`) is "approval only" and rung `none` is "not armed", so
--   a row claiming either would be a record of a rule violation. The two
--   permitted values are `services::assertion_ladder::Rung::label`'s own
--   strings, so the column and the evidence agree by construction.
--
--   `evidence JSONB` — the same free-form bag `org_join_requests.evidence`
--   is (0238): the full ladder verdict, the matched domain, the tier and
--   the slot label. Nothing in it is queried by value; it is rendered back
--   to an admin.
--
--   No uniqueness on `(account_id, user_id)`: a person auto-added, later
--   removed, and auto-added again is TWO events and the digest should
--   report the second one. (A removal writes `org_membership_tombstones`,
--   which blocks the second auto-add outright — D12 — so in practice the
--   second row only happens after an admin has cleared the tombstone. It is
--   still two facts, not one.)
--
--   `seat_tier_id` records the tier actually spent, alongside the rung —
--   the two questions an admin asks about an add they did not make are
--   "why was this allowed" and "what did it cost".
--
--   ── (4) `org_join_request_digests.auto_add_count` ─────────────────────
--
--   D13 as amended 2026-09-22: the existing daily digest gains a second
--   section. The idempotency does NOT change — `(account_id,
--   admin_user_id, sent_on)` stays the PRIMARY KEY and stays the ONLY thing
--   that makes a digest once-a-day — so the new section rides on the same
--   claim-then-send-then-commit row rather than getting a table, a claim,
--   or a cadence of its own.
--
--   0240's `CHECK (request_count > 0)` encoded a real invariant — *we never
--   write a digest row for a digest we had no reason to send* — but it
--   encoded it in terms of the only section that existed. With two
--   sections, a digest sent because three people were auto-added and nobody
--   asked has `request_count = 0`, which that CHECK refuses. So the
--   invariant is re-stated over BOTH counts rather than dropped:
--
--     request_count >= 0 AND auto_add_count >= 0   (neither is negative)
--     request_count + auto_add_count > 0           (and a row means a send)
--
--   The second of those is 0240's constraint generalised, not weakened: an
--   empty digest is still unrepresentable. Dropping and re-adding a CHECK
--   on a table an earlier migration created is the only way to evolve one
--   (0240 itself is merged and immutable — its bytes are in the sqlx
--   ledger of every deployed database).
--
--   `DEFAULT 0` on a `NOT NULL` add: every existing row predates auto-add
--   and reported zero auto-adds, so the default is the true value for
--   history, not a placeholder.
--
--   ── RLS: `org_auto_add_settings` and `org_auto_adds` mirror 0238/0242 ──
--
--   Account-level operational state with no `graph_id`/ocean concept — a
--   bare account-scoped `account_isolation` policy covering every command,
--   the shape 0227/0233/0238/0242's own headers describe. Control-only, no
--   tenant-tree twin: a toggle and an add-ledger are bookkeeping about an
--   ORG, never read back as ocean content, and every gate that reads them
--   runs on the control plane before any tenant pool is chosen.
--
--   Both are registered in this same PR in all FOUR rosters — the discipline
--   0242's header records after 0238/0240 each missed one:
--     (a) `EXTRA_RLS_TABLES` in `scripts/check_rls_coverage.sh` (that script
--         derives its automatic roster from `0083_consumer_rls.sql`'s own
--         `CREATE POLICY` lines, so a policy added by a LATER migration is
--         never in it);
--     (b) `RLS_TABLES` in `backend/tests/consumer_rls_pg.rs` (asserted as a
--         SET against the live database, so a missing entry is a red test,
--         not a silent gap);
--     (c) `CONTROL_TABLES` in `scripts/check_schema_parity.sh` (control-only,
--         must never grow a tenant twin);
--     (d) every production reader/writer opens
--         `tenancy::scope::begin_account_scoped` first — `services::
--         org_auto_add` has no other way to touch either table, and
--         `state.control_db()` is `coralswarm_app` (`rolbypassrls = f`), so
--         an unscoped query hard-errors against these policies rather than
--         silently returning nothing.
--
-- Depends on: 0001_init (accounts, users), 0165_seat_tiers (seat_tiers),
--             0240_org_join_request_digests (the table altered below),
--             0242_org_domains (the table indexed below),
--             0083_consumer_rls (the account_isolation policy convention).
-- Down:
--   ALTER TABLE org_join_request_digests DROP CONSTRAINT IF EXISTS org_join_request_digests_counts_nonneg;
--   ALTER TABLE org_join_request_digests DROP CONSTRAINT IF EXISTS org_join_request_digests_reports_something;
--   ALTER TABLE org_join_request_digests DROP COLUMN IF EXISTS auto_add_count;
--   ALTER TABLE org_join_request_digests ADD CONSTRAINT org_join_request_digests_request_count_check CHECK (request_count > 0);
--   DROP POLICY IF EXISTS account_isolation ON org_auto_adds;
--   DROP TABLE IF EXISTS org_auto_adds;
--   DROP POLICY IF EXISTS account_isolation ON org_auto_add_settings;
--   DROP TABLE IF EXISTS org_auto_add_settings;
--   DROP INDEX IF EXISTS org_domains_verified_domain_global_idx;

-- ── (1) One verified org per domain, globally ─────────────────────────────
-- Partial: unverified CLAIMS stay per-account (0242's
-- `org_domains_account_domain_idx` is untouched), only PROOF is exclusive.
CREATE UNIQUE INDEX IF NOT EXISTS org_domains_verified_domain_global_idx
    ON org_domains (domain)
    WHERE verified_at IS NOT NULL;

-- ── (2) The toggle, and the tier it is allowed to spend ───────────────────
CREATE TABLE IF NOT EXISTS org_auto_add_settings (
    account_id           UUID        PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
    -- D11: off by default. A MISSING ROW is also off — see the header.
    enabled              BOOLEAN     NOT NULL DEFAULT false,
    -- Founder decision 2026-09-22: the admin picks the tier on the toggle.
    -- NULL = `standard`, the same NULL semantics 0241 gave
    -- `slot_reservations.seat_tier_id`. NO ACTION on delete on purpose: a
    -- tier vanishing must not silently re-point this org's spend.
    default_seat_tier_id UUID        NULL REFERENCES seat_tiers(id),
    updated_by           UUID        NULL REFERENCES users(id) ON DELETE SET NULL,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE org_auto_add_settings ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON org_auto_add_settings;
CREATE POLICY account_isolation ON org_auto_add_settings
    USING (account_id = current_setting('app.account_id')::uuid)
    WITH CHECK (account_id = current_setting('app.account_id')::uuid);

-- ── (3) The ledger of adds no human made ──────────────────────────────────
CREATE TABLE IF NOT EXISTS org_auto_adds (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id   UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    user_id      UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    -- D16's top two rungs, and ONLY those two — `services::assertion_
    -- ladder::Rung::label`'s own strings. Rung 3 is approval-only and rung
    -- `none` is not-armed, so either would be a record of a rule violation.
    rung         TEXT        NOT NULL CHECK (rung IN ('directory', 'member_and_domain')),
    -- The tier actually spent (NULL = `standard`, as everywhere else).
    seat_tier_id UUID        NULL REFERENCES seat_tiers(id),
    evidence     JSONB       NOT NULL DEFAULT '{}',
    added_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The digest's own read: "who was auto-added for this org since <timestamp>",
-- oldest first (`workers::org_join_request_digest::new_auto_adds_since`).
CREATE INDEX IF NOT EXISTS org_auto_adds_by_account_added_idx
    ON org_auto_adds (account_id, added_at);

ALTER TABLE org_auto_adds ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON org_auto_adds;
CREATE POLICY account_isolation ON org_auto_adds
    USING (account_id = current_setting('app.account_id')::uuid)
    WITH CHECK (account_id = current_setting('app.account_id')::uuid);

-- ── (4) The digest's second section ───────────────────────────────────────
ALTER TABLE org_join_request_digests
    ADD COLUMN IF NOT EXISTS auto_add_count INT NOT NULL DEFAULT 0;

-- 0240's `request_count > 0` re-stated over BOTH sections: neither count is
-- negative, and a digest row still means a digest was actually sent.
ALTER TABLE org_join_request_digests
    DROP CONSTRAINT IF EXISTS org_join_request_digests_request_count_check;
ALTER TABLE org_join_request_digests
    DROP CONSTRAINT IF EXISTS org_join_request_digests_counts_nonneg;
ALTER TABLE org_join_request_digests
    ADD CONSTRAINT org_join_request_digests_counts_nonneg
    CHECK (request_count >= 0 AND auto_add_count >= 0);
ALTER TABLE org_join_request_digests
    DROP CONSTRAINT IF EXISTS org_join_request_digests_reports_something;
ALTER TABLE org_join_request_digests
    ADD CONSTRAINT org_join_request_digests_reports_something
    CHECK (request_count + auto_add_count > 0);
