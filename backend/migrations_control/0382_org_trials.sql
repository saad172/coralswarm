-- Migration: 0382_org_trials
-- Description: Self-serve org signup, PR C′ — the free-credit trial a new
--   team org gets when its creator completes the card-up-front Stripe
--   Checkout (founder decisions 2026-10-27, #1 and #9).
--
--   1. `org_trials` — one row per self-serve team org, carrying the trial
--      lifecycle as ONE explicit state column (never a set of booleans):
--
--        checkout_open ──► trialing ──► converting ──► converted
--              │              │             │
--              ▼              ▼             ▼
--          abandoned       canceled      canceled
--
--      plus two shortcuts out of `checkout_open`: straight to `converted`
--      (a creator who already had a trial gets a Checkout with no trial and
--      pays the seats on day 1) and straight to `converting` (the card's
--      fingerprint already had a trial: the trial is ended at once). The
--      legal transitions are owned by `billing::org_trial::TrialState`; the
--      CHECKs below only pin which columns each state must carry.
--
--      The trial's credits are ONE flat org pool (3,000 by default,
--      whatever the seat count): a positive `credit_ledger` row
--      (`entry_kind = 'trial_credit'`) raises `credit_balances.balance`, and
--      every plan debit already draws that balance down
--      (`workers::billing_derivation`). `pool_baseline` is the balance right
--      after the credit landed, so `used = pool_baseline - balance` without
--      re-summing the ledger.
--
--   2. `org_trial_claims` — who has already had a trial: one row per
--      creator user and one per Stripe card fingerprint (decision #9: one
--      trial per user AND per card). Deliberately NOT keyed to the account
--      with a cascading FK: deleting an org must not hand its creator, or
--      the card, a second trial. The app role holds no privilege on the
--      table at all; it is read and written only through the two narrow
--      SECURITY DEFINER functions below, so a request path can ask "has this
--      user or card trialed?" without being able to list anyone's claims.
--
--   3. `org_trial_accounts_due()` — the account ids the trial worker has to
--      look at (a live lifecycle state). `org_trials` is RLS-covered, so a
--      cross-account scan under the app role needs this carve-out; it
--      returns bare ids and nothing else (same shape as 0086's resolvers).
--
--   4. `billing_notices.notice_kind` gains the three trial emails: 80% used,
--      100% used (the trial converts), and the 90-day backstop warning sent
--      7 days before the trial converts anyway.
--
--   ── RLS / GRANTS ──
--   `org_trials` takes the strict `account_isolation` policy (USING + WITH
--   CHECK on app.account_id, as 0367) and the 0321 financial entry fence:
--   it gates money (whether usage is free, when the card is charged), so
--   every write must run under `billing_enter_account` like the other
--   billing tables. The app role gets exactly SELECT, INSERT and UPDATE on
--   it: 0083's default privileges would also hand it DELETE, TRUNCATE,
--   REFERENCES and TRIGGER, which are revoked and asserted absent (the app
--   never deletes a trial row; it goes with its account through the FK
--   cascade, which runs as the table owner).
--   `org_trial_claims` has every privilege REVOKEd from the app role.
--   Both tables are control-only (CONTROL_TABLES in check_schema_parity.sh).
--
--   ── DEFINERS ──
--   `SET search_path = pg_catalog, public, pg_temp` and schema-qualified
--   relations, per 0376.

-- ── 1. org_trials ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.org_trials (
    account_id             UUID        PRIMARY KEY REFERENCES public.accounts(id) ON DELETE CASCADE,
    state                  TEXT        NOT NULL CHECK (state IN (
                               'checkout_open', 'trialing', 'converting',
                               'converted', 'abandoned', 'canceled')),
    -- The signed-in user who created the org; their trial claim is taken at
    -- Checkout completion. Nullable so deleting the user keeps the row.
    creator_user_id        UUID        REFERENCES public.users(id) ON DELETE SET NULL,
    -- Seats bought at Checkout (team orgs start at 2, decision #5).
    seats                  INTEGER     NOT NULL CHECK (seats >= 2),
    -- Decided when the Checkout was created: false when the creator had
    -- already trialed, which makes that Checkout charge on day 1.
    trial_eligible         BOOLEAN     NOT NULL,
    checkout_session_id    TEXT        CHECK (checkout_session_id IS NULL OR checkout_session_id <> ''),
    -- Anchor of the 24 h "resume payment" window; set once, never moved by
    -- a resume.
    checkout_opened_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    stripe_subscription_id TEXT        CHECK (stripe_subscription_id IS NULL OR stripe_subscription_id <> ''),
    card_fingerprint       TEXT        CHECK (card_fingerprint IS NULL OR card_fingerprint <> ''),
    trial_credits          NUMERIC     NOT NULL DEFAULT 3000 CHECK (trial_credits > 0),
    pool_baseline          NUMERIC,
    trial_started_at       TIMESTAMPTZ,
    -- Stripe's own `trial_end`: the 90-day backstop. Stripe ends the trial
    -- at this instant even if our worker never runs.
    trial_ends_at          TIMESTAMPTZ,
    convert_reason         TEXT        CHECK (convert_reason IS NULL OR convert_reason IN (
                               'credits_used', 'backstop', 'repeat_card', 'no_trial', 'card_unverified')),
    convert_requested_at   TIMESTAMPTZ,
    -- Last time the worker saw the conversion invoice unpaid (dunning).
    payment_failed_at      TIMESTAMPTZ,
    converted_at           TIMESTAMPTZ,
    -- The first successful charge for this org: the conversion, a
    -- no-trial Checkout, or a later subscription's first paid invoice after
    -- the trial ended unpaid. Decision #6: extra usage and auto-reload stay
    -- refused until it is set.
    first_charge_at        TIMESTAMPTZ,
    ended_at               TIMESTAMPTZ,
    end_reason             TEXT        CHECK (end_reason IS NULL OR end_reason <> ''),
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT org_trials_trialing_has_pool CHECK (
        state NOT IN ('trialing') OR (
            trial_started_at IS NOT NULL AND trial_ends_at IS NOT NULL
            AND pool_baseline IS NOT NULL AND stripe_subscription_id IS NOT NULL)),
    CONSTRAINT org_trials_converting_has_reason CHECK (
        state NOT IN ('converting', 'converted') OR (
            convert_reason IS NOT NULL AND convert_requested_at IS NOT NULL
            AND stripe_subscription_id IS NOT NULL)),
    CONSTRAINT org_trials_converted_has_time CHECK (
        (state = 'converted') = (converted_at IS NOT NULL)),
    CONSTRAINT org_trials_ended_has_reason CHECK (
        (state IN ('abandoned', 'canceled')) = (ended_at IS NOT NULL AND end_reason IS NOT NULL))
);

-- The worker's scan (through `org_trial_accounts_due`) reads only live rows.
CREATE INDEX IF NOT EXISTS org_trials_live_idx
    ON public.org_trials (state) WHERE state IN ('checkout_open', 'trialing', 'converting');

ALTER TABLE public.org_trials ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON public.org_trials;
CREATE POLICY account_isolation ON public.org_trials
    USING (account_id = current_setting('app.account_id')::uuid)
    WITH CHECK (account_id = current_setting('app.account_id')::uuid);

DROP TRIGGER IF EXISTS billing_current_generation ON public.org_trials;
CREATE TRIGGER billing_current_generation BEFORE INSERT OR UPDATE OR DELETE ON public.org_trials
    FOR EACH ROW EXECUTE FUNCTION public.billing_guard_account_row();
DROP TRIGGER IF EXISTS billing_no_truncate ON public.org_trials;
CREATE TRIGGER billing_no_truncate BEFORE TRUNCATE ON public.org_trials
    FOR EACH STATEMENT EXECUTE FUNCTION public.managed_refuse_truncate();

-- ── 2. org_trial_claims ──────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.org_trial_claims (
    claim_kind  TEXT        NOT NULL CHECK (claim_kind IN ('user', 'card')),
    -- `users.id` for a user claim, the Stripe card fingerprint for a card.
    claim_value TEXT        NOT NULL CHECK (claim_value <> ''),
    -- The org that used the trial. No FK: the claim outlives the org.
    account_id  UUID        NOT NULL,
    claimed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (claim_kind, claim_value)
);

-- ── 3. SECURITY DEFINER carve-outs ───────────────────────────────────────

-- 3a. Has this user, or this card, already had a trial? Either argument may
-- be NULL (the card is unknown before Checkout; the user may be gone).
CREATE OR REPLACE FUNCTION public.org_trial_already_claimed(p_user UUID, p_card_fingerprint TEXT)
RETURNS BOOLEAN
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
    SELECT EXISTS (
        SELECT 1 FROM public.org_trial_claims c
         WHERE (p_user IS NOT NULL AND c.claim_kind = 'user' AND c.claim_value = p_user::text)
            OR (p_card_fingerprint IS NOT NULL AND p_card_fingerprint <> ''
                AND c.claim_kind = 'card' AND c.claim_value = p_card_fingerprint))
$$;

-- 3b. Take the trial for `p_account`: record the user claim and the card
-- claim together, or neither. Returns false (writing nothing) when either is
-- already taken, including by a concurrent caller that commits first (the
-- primary key decides; the unique violation is caught in a subtransaction).
-- Only callable for the account the caller's transaction is scoped to.
CREATE OR REPLACE FUNCTION public.org_trial_claim(p_account UUID, p_user UUID, p_card_fingerprint TEXT)
RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
BEGIN
    IF p_account IS NULL OR p_account::text IS DISTINCT FROM current_setting('app.account_id', true) THEN
        RAISE EXCEPTION 'org_trial_claim: account % is not the scoped account', p_account;
    END IF;
    IF p_user IS NULL AND (p_card_fingerprint IS NULL OR p_card_fingerprint = '') THEN
        RAISE EXCEPTION 'org_trial_claim: nothing to claim';
    END IF;
    BEGIN
        IF p_user IS NOT NULL THEN
            INSERT INTO public.org_trial_claims (claim_kind, claim_value, account_id)
            VALUES ('user', p_user::text, p_account);
        END IF;
        IF p_card_fingerprint IS NOT NULL AND p_card_fingerprint <> '' THEN
            INSERT INTO public.org_trial_claims (claim_kind, claim_value, account_id)
            VALUES ('card', p_card_fingerprint, p_account);
        END IF;
    EXCEPTION WHEN unique_violation THEN
        RETURN false;
    END;
    RETURN true;
END $$;

-- 3c. Accounts whose trial lifecycle still needs the worker. Bare ids only.
CREATE OR REPLACE FUNCTION public.org_trial_accounts_due()
RETURNS SETOF UUID
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
    SELECT t.account_id FROM public.org_trials t
     WHERE t.state IN ('checkout_open', 'trialing', 'converting')
     ORDER BY t.account_id
$$;

REVOKE ALL ON FUNCTION public.org_trial_already_claimed(UUID, TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.org_trial_claim(UUID, UUID, TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.org_trial_accounts_due() FROM PUBLIC;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'coralswarm_app') THEN
        REVOKE ALL ON public.org_trial_claims FROM coralswarm_app;
        REVOKE ALL ON public.org_trials FROM coralswarm_app;
        GRANT SELECT, INSERT, UPDATE ON public.org_trials TO coralswarm_app;
        GRANT EXECUTE ON FUNCTION public.org_trial_already_claimed(UUID, TEXT) TO coralswarm_app;
        GRANT EXECUTE ON FUNCTION public.org_trial_claim(UUID, UUID, TEXT) TO coralswarm_app;
        GRANT EXECUTE ON FUNCTION public.org_trial_accounts_due() TO coralswarm_app;
        IF has_table_privilege('coralswarm_app', 'public.org_trial_claims', 'SELECT,INSERT,UPDATE,DELETE') THEN
            RAISE EXCEPTION 'coralswarm_app must not hold any privilege on org_trial_claims';
        END IF;
        IF has_table_privilege('coralswarm_app', 'public.org_trials', 'DELETE,TRUNCATE,REFERENCES,TRIGGER') THEN
            RAISE EXCEPTION 'coralswarm_app must hold only SELECT, INSERT, UPDATE on org_trials';
        END IF;
        IF NOT (has_table_privilege('coralswarm_app', 'public.org_trials', 'SELECT')
                AND has_table_privilege('coralswarm_app', 'public.org_trials', 'INSERT')
                AND has_table_privilege('coralswarm_app', 'public.org_trials', 'UPDATE')) THEN
            RAISE EXCEPTION 'coralswarm_app needs SELECT, INSERT, UPDATE on org_trials';
        END IF;
    END IF;
END $$;

-- ── 4. Trial emails ──────────────────────────────────────────────────────
ALTER TABLE public.billing_notices DROP CONSTRAINT IF EXISTS billing_notices_notice_kind_check;
ALTER TABLE public.billing_notices ADD CONSTRAINT billing_notices_notice_kind_check CHECK (notice_kind IN (
    'extra_usage_limit_reached', 'member_cap_reached', 'balance_depleted',
    'auto_reload_succeeded', 'auto_reload_failed',
    'trial_credits_80', 'trial_credits_used', 'trial_ending_soon'));
