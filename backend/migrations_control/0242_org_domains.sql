-- Migration: 0242_org_domains
-- Description: CS-443 (org-onboarding plan §3 item 37,
--   `docs/plans/org-onboarding-from-the-invite.md`, D11/D16/D21) — the org's
--   claimed and verified email domains. Until this table existed there was
--   NO org-domain concept in the codebase at all: `email_domain` (three
--   private copies, in `workers::org_join_request_digest`,
--   `services::unmatched_recipient_email` and friends) is a PII-redaction
--   helper for log lines, nothing more, and D11's own text says so in as
--   many words. This is the table that makes "an @acme.com address belongs
--   to Acme" a fact the server can check instead of a string an admin typed.
--
--   ── D11 — verified, or it is a text field someone types `gmail.com` into ──
--   Auto-add (item 9, NOT built in this PR) applies only to addresses
--   matching a VERIFIED org domain; everyone else falls to approval. So a
--   row in this table is worth nothing until `verified_at` is stamped, and
--   every production reader that gates on a domain reads
--   `verified_at IS NOT NULL`, never mere existence
--   (`services::org_domains::verified_domains` is the ONE such reader, and
--   `connectors::slack::verified_org_domain` is its one caller — see that
--   function's own doc comment: a claimed-but-unverified domain returning
--   `Some(...)` would silently widen D2's guest rule, which is exactly the
--   bug this column shape exists to make impossible).
--
--   ── D11 — "the org's settings show which one proved it" ──
--   Two methods prove control, decided 2026-09-20: a DNS TXT record (`method
--   = 'dns_txt'`) for orgs with IT, or a confirmation link clicked at an
--   address AT that domain (`method = 'email'`) for everyone else. D11 does
--   not treat the two as interchangeable bookkeeping — it requires the org's
--   settings to SHOW which one proved it, so `method` is a stored fact about
--   the verification, not something re-derived later from whichever columns
--   happen to be populated.
--
--   `CHECK ((verified_at IS NULL) = (method IS NULL))` is the same paired-
--   nullability invariant `org_join_requests`' own `CHECK ((decided_at IS
--   NULL) = (decision IS NULL))` (0238) and `slot_reservations`' own
--   `CHECK ((released_at IS NULL) = (release_reason IS NULL))` (0233)
--   enforce, for the identical reason: "verified but by no method", and
--   "proved by DNS but not verified", are states this table can never
--   represent, so the DB refuses them rather than trusting every writer to
--   keep the two in sync. The second `CHECK` pairs
--   `confirmation_email`/`confirmation_sent_at` the same way.
--
--   ── `verification_token` — minted at CLAIM time, not at verify time ──
--   The token is what the admin publishes in DNS (`_coralswarm-verification.
--   <domain>  IN TXT  "coralswarm-domain-verification=<token>"`), so it must
--   exist and be stable from the moment the domain is claimed — an admin
--   publishes the record, waits for propagation, and clicks verify minutes
--   or hours later. It is also what the emailed confirmation link's signed
--   JWT is bound to, so that a token minted for one claim cannot verify a
--   re-claim of the same domain after a removal. Re-claiming an existing
--   UNVERIFIED row returns the SAME token rather than rotating it (a second
--   click on "add domain" must not invalidate the record the admin already
--   published).
--
--   ── `confirmation_email` / `confirmation_sent_at` — the send claim ──
--   `routes::org_domains::request_confirmation` claims these two columns in
--   a transaction, sends through Resend, and commits ONLY on a successful
--   send (rolling back otherwise) — the same claim-then-send-then-commit
--   order `services::unmatched_recipient_email::claim_notice_slot` and
--   `workers::org_join_request_digest::process_account` both use, for the
--   same reason: a failed send must never consume the state that would stop
--   the next attempt. `confirmation_sent_at` doubles as the resend cooldown.
--
--   When `method = 'email'`, `confirmation_email` IS the address that proved
--   the domain: an already-verified row refuses further confirmation sends
--   (`routes::org_domains::request_confirmation` returns 409), so the column
--   cannot be overwritten after the fact by a send aimed somewhere else.
--   When `method = 'dns_txt'` the column is whatever the last unsuccessful
--   email attempt (if any) was aimed at, and the settings surface must not
--   render it as proof — `method` is the only thing that says what proved
--   this row.
--
--   ── Shape: one row per (account, domain), no global uniqueness ──
--   Uniqueness is `(account_id, domain)`, deliberately NOT `domain` alone.
--   Two accounts genuinely can both control the same domain (a parent and a
--   subsidiary; an agency and the client whose mail it runs), and BOTH would
--   have to independently prove it here — publish a TXT record carrying
--   THEIR OWN `verification_token`, or click a link mailed to an address at
--   that domain. A global unique index would let whoever claimed first
--   permanently deny a second, equally-legitimate org, which is a worse
--   failure than two verified rows. D11 does not settle this; it is called
--   out in CS-443's PR body rather than decided silently here.
--
--   `claimed_by ... ON DELETE SET NULL` (rather than 0238's `decided_by
--   UUID REFERENCES users(id)` with the default NO ACTION): a verified
--   domain is a fact about the ORG and must outlive the admin who claimed
--   it — an org losing its domain verification because the person who set it
--   up was deleted would silently disarm auto-add for everyone. The org's
--   own `ON DELETE CASCADE` on `account_id` is the one deletion that SHOULD
--   take these rows with it.
--
--   ── RLS: mirrors 0238 exactly ──
--   Account-level operational state with no `graph_id`/ocean concept — a
--   bare account-scoped `account_isolation` policy covering every command,
--   the same shape 0227/0233/0238's own headers describe for their own
--   tables. Control-only, no tenant-tree twin: a claimed domain is
--   bookkeeping about an ORG, never read back as ocean content, and the
--   auto-add/guest-rule gates that read it run on the control plane before
--   any tenant pool is chosen.
--
--   0238's header records the two things CodeRabbit round 1 (PR #1021) had
--   to force for the identical shape, and this migration does BOTH in its
--   own PR rather than waiting to be told:
--     (a) every production reader/writer opens
--         `tenancy::scope::begin_account_scoped` first —
--         `services::org_domains` has no other way to touch this table, and
--         `state.control_db()` is `coralswarm_app` (`rolbypassrls = f`), so
--         an unscoped query hard-errors against this policy rather than
--         silently returning nothing;
--     (b) `org_domains` is added to `scripts/check_rls_coverage.sh`'s
--         hand-curated `EXTRA_RLS_TABLES` list in this same PR. That script
--         derives its AUTOMATIC roster from `0083_consumer_rls.sql`'s own
--         `CREATE POLICY` lines alone, so a policy added by a LATER
--         migration — like this one — is never in it; the hand-curated list
--         is the permanent fix `slack_unmatched_email_notices`,
--         `org_join_requests` and `org_join_request_digests` already use.
--   Added to `CONTROL_TABLES` in `scripts/check_schema_parity.sh` in this
--   same PR, for the same "control-only, must never grow a tenant twin"
--   reason.
--
-- Depends on: 0001_init (accounts, users), 0083_consumer_rls (the
--             account_isolation-shaped policy convention),
--             0233_slot_reservations / 0238_org_join_requests (the
--             direct-account_id, control-only, paired-nullability-CHECK
--             precedent this migration follows).
-- Down:
--   DROP POLICY IF EXISTS account_isolation ON org_domains;
--   DROP TABLE IF EXISTS org_domains;

CREATE TABLE IF NOT EXISTS org_domains (
    id                   UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id           UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    -- Normalised by `services::org_domains::normalize_domain` before it ever
    -- reaches this column: trimmed, lowercased, a leading `@` and a trailing
    -- root dot stripped, `https://`-style prefixes and paths refused
    -- outright. Stored normalised so the `(account_id, domain)` unique index
    -- and the auto-add/guest-rule comparisons are one and the same string.
    domain               TEXT        NOT NULL,
    -- Published by the admin as a DNS TXT value, and bound into the emailed
    -- confirmation JWT. Minted once at claim time; never rotated by a
    -- re-claim of the same still-unverified row.
    verification_token   TEXT        NOT NULL,
    verified_at          TIMESTAMPTZ NULL,
    -- WHICH proof succeeded (D11) — NULL exactly while unverified.
    method               TEXT        NULL CHECK (method IN ('dns_txt', 'email')),
    claimed_by           UUID        REFERENCES users(id) ON DELETE SET NULL,
    claimed_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- The address the most recent confirmation email was aimed at, and when
    -- it was claimed — see the header's "the send claim" section.
    confirmation_email   TEXT        NULL,
    confirmation_sent_at TIMESTAMPTZ NULL,

    CHECK ((verified_at IS NULL) = (method IS NULL)),
    CHECK ((confirmation_sent_at IS NULL) = (confirmation_email IS NULL))
);

-- One claim per domain per org. NOT global (see the header): two orgs may
-- each independently prove control of the same domain.
CREATE UNIQUE INDEX IF NOT EXISTS org_domains_account_domain_idx
    ON org_domains (account_id, domain);

-- The hot read: "does this org have a verified domain, and which"
-- (`services::org_domains::verified_domains`, behind
-- `connectors::slack::verified_org_domain` and the assertion ladder's own
-- rung-2 domain check).
CREATE INDEX IF NOT EXISTS org_domains_verified_by_account_idx
    ON org_domains (account_id, verified_at)
    WHERE verified_at IS NOT NULL;

ALTER TABLE org_domains ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON org_domains;
CREATE POLICY account_isolation ON org_domains
    USING (account_id = current_setting('app.account_id')::uuid)
    WITH CHECK (account_id = current_setting('app.account_id')::uuid);
