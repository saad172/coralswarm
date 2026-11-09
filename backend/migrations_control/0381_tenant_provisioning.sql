-- Migration: 0381_tenant_provisioning
-- Description: Self-serve org signup, PR A1 — the control-plane schema for
--   automated tenant provisioning (docs/plans/org-selfserve-signup-audit.md,
--   rows 4a-4g). Three things land here:
--
--   1. `tenant_db_credentials` — a KEK-encrypted tenant DSN keyed by the
--      routing table's `db_secret_ref`. The successor to "one Fly secret per
--      tenant" (org-tenancy plan, open item 3): a running process can read a
--      brand-new tenant's DSN without a fleet restart. The plaintext never
--      lands here — `tenancy::credentials` AES-256-GCM-encrypts it under
--      MASTER_KEK_HEX before INSERT and decrypts after SELECT. The resolver
--      chain (`tenancy::secrets::ChainSecretResolver`) consults the
--      environment FIRST, so every existing concierge tenant resolves exactly
--      as before; this table is only ever reached for a ref the environment
--      does not carry.
--
--   2. `tenant_provisioning_jobs` — one durable, resumable job per org: the
--      step the job is on, its status, bounded-retry bookkeeping, a lease so
--      two backend machines never run the same job at once, and a record of
--      what the job created (so cleanup deletes exactly that and nothing
--      else). The step/status vocabulary is owned by
--      `tenancy::provisioning`; this migration only constrains the
--      shape. (Folds the stale #1391/#1392 migrations 0352-0354 into one file;
--      none of them ever merged.)
--
--   3. SECURITY DEFINER functions for every write a provisioning job needs,
--      to `tenant_databases`, `tenant_db_credentials` AND
--      `tenant_provisioning_jobs`. Migration 0083 REVOKEs INSERT/UPDATE/DELETE
--      on `tenant_databases` from `coralswarm_app` (the serving role), and
--      that hardening stays: an app-layer compromise must not be able to
--      repoint an org's database. The routing definers read the job row, so
--      the job row gets the same treatment: the app role holds SELECT only on
--      it, and every job write is one of the narrow functions below (enqueue
--      derives the slug and ref itself; step changes follow the forward
--      path; the provider id is recorded once). `insert_routing` re-derives
--      the self-serve shape (opaque slug and ref from the account id), and
--      requires a live subscription and an org that is not already routed,
--      so even a forged job row cannot repoint an existing org.
--
--   4. A role split. The serving role (`coralswarm_app`) can do everything
--      `insert_routing` checks for itself (it writes `stripe_subscriptions`
--      and can call `billing_enter_account`), so the routing-writing
--      definers are NOT granted to it. They go to a new NOLOGIN group role,
--      `coralswarm_provisioner`, which only the provisioning worker's own
--      control pool (`PROVISIONER_DATABASE_URL`) logs in as. That secret is
--      app-wide on Fly (the `http` machines carry it too), so the split
--      stops the serving role being misused through SQL; it does not stop
--      code execution on an `http` machine. The app role keeps `enqueue`, `cancel`, `retry`,
--      `mark_clerk_org_cleanup_done` and SELECT; it can ask for an org to be
--      provisioned, never advance the job or write a routing row.
--      `scripts/check_provisioning_privileges.sh` (a pre-deploy guard)
--      re-checks this split on every deploy against a fresh database built
--      from the tree (it catches a migration that undoes it, not a grant
--      made by hand in prod).
--
--   Migration 0321's `billing_guard_tenant_binding` trigger fires on INSERT,
--   on DELETE, and on any UPDATE that changes `db_secret_ref`: it requires
--   `app.account_id` to equal the row's account AND a `billing_enter_account`
--   entry in the same transaction, and (on INSERT/UPDATE) an `accounts` row
--   that is an org account carrying the same `clerk_org_id`. The insert and
--   delete functions therefore enter the account themselves; the
--   status/schema_version updates do not need to (the trigger returns early
--   when the binding columns are unchanged). The `accounts` row is a
--   PRECONDITION the job verifies before it reaches these functions, never
--   something they create.
--
-- Registered in `scripts/check_schema_parity.sh`'s CONTROL_TABLES in this
-- same PR. No RLS: neither table carries user content, and both are read
-- only by the auth spine / the provisioning worker.
--
-- Down:
--   DROP FUNCTION IF EXISTS tenant_provisioning_retry(text);
--   DROP FUNCTION IF EXISTS tenant_provisioning_mark_clerk_org_cleanup_done(text);
--   DROP FUNCTION IF EXISTS tenant_provisioning_cancel(text);
--   DROP FUNCTION IF EXISTS tenant_provisioning_fail(text, text, text, boolean);
--   DROP FUNCTION IF EXISTS tenant_provisioning_record_credential_stored(text, text, interval);
--   DROP FUNCTION IF EXISTS tenant_provisioning_record_database(text, text, interval, text, text);
--   DROP FUNCTION IF EXISTS tenant_provisioning_clear_create_intent(text, text, interval);
--   DROP FUNCTION IF EXISTS tenant_provisioning_record_create_intent(text, text, interval);
--   DROP FUNCTION IF EXISTS tenant_provisioning_defer(text, text, integer, text, interval, text);
--   DROP FUNCTION IF EXISTS tenant_provisioning_set_step(text, text, interval, text, text);
--   DROP FUNCTION IF EXISTS tenant_provisioning_claim(text, interval, text);
--   DROP FUNCTION IF EXISTS tenant_provisioning_enqueue(text, uuid, text);
--   DROP FUNCTION IF EXISTS tenant_provisioning_delete_routing(text);
--   DROP FUNCTION IF EXISTS tenant_provisioning_activate(text, bigint);
--   DROP FUNCTION IF EXISTS tenant_provisioning_record_schema_version(text, text, interval, bigint);
--   DROP FUNCTION IF EXISTS tenant_provisioning_insert_routing(text, uuid, text, text, text);
--   DROP FUNCTION IF EXISTS tenant_provisioning_delete_credential(text);
--   DROP FUNCTION IF EXISTS tenant_provisioning_store_credential(text, bytea);
--   DROP TABLE IF EXISTS tenant_provisioning_jobs;
--   DROP TABLE IF EXISTS tenant_db_credentials;
--   -- The role is cluster-wide; drop it only once no database uses it:
--   -- DROP ROLE IF EXISTS coralswarm_provisioner;

-- Bound every lock wait (the repo convention for control migrations): the
-- GRANT/REVOKE on `tenant_databases` below queue behind any long transaction
-- on it, and every routed request reads that table.
SET LOCAL lock_timeout = '3s';

-- ── 1. Encrypted tenant DSNs ──────────────────────────────────────────────────

CREATE TABLE tenant_db_credentials (
    -- Same vocabulary as `tenant_databases.db_secret_ref` (an env-var-shaped
    -- name, e.g. TENANT_DB_O_0123456789ABCDEF), and only that vocabulary: a
    -- `TENANT_DB_` name, so a row here can never answer for any other secret
    -- the resolver chain is asked for. Not a foreign key: the credential is
    -- written BEFORE the routing row exists (so a release that runs
    -- `migrate_tenants` between the two can still resolve it) and deleted
    -- AFTER the routing row is gone.
    secret_ref    TEXT        PRIMARY KEY
                              CHECK (secret_ref ~ '^TENANT_DB_[A-Z0-9_]{1,118}$'),
    -- AES-256-GCM under MASTER_KEK_HEX: [nonce 12B][tag 16B][ciphertext],
    -- with associated data `tenant-dsn:v1:<secret_ref>`, so a ciphertext
    -- moved to another row does not decrypt there. Never the plaintext DSN. Decrypt is rotation-aware (MASTER_KEK_HEX_NEXT
    -- is tried first), exactly like every other KEK unwrap site.
    dsn_encrypted BYTEA       NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── 2. The job table ──────────────────────────────────────────────────────────

CREATE TABLE tenant_provisioning_jobs (
    -- One job per org, ever: a retry resumes THIS row, it never mints a
    -- second one (and so never a second Neon project).
    clerk_org_id         TEXT        PRIMARY KEY,
    account_id           UUID        NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
    -- Self-serve only, so both are DERIVED from the account id, never
    -- chosen: `o-` + its first 16 hex digits
    -- (`tenancy::provisioning::opaque_slug_for_account`) and `TENANT_DB_` +
    -- that slug upper-cased, `-` → `_` (`secret_ref_for_slug`). The table
    -- CHECKs below pin both derivations.
    slug                 TEXT        NOT NULL
                                     CHECK (slug ~ '^o-[0-9a-f]{16}$'),
    db_secret_ref        TEXT        NOT NULL
                                     CHECK (db_secret_ref ~ '^TENANT_DB_[A-Z0-9_]{1,118}$'),
    region               TEXT,
    -- The step the job will run NEXT (`tenancy::provisioning::Step`). The
    -- vocabulary lives in Rust; the CHECK keeps a typo from parking a job in
    -- a state no code will ever pick up.
    step                 TEXT        NOT NULL DEFAULT 'verify_org'
                                     CHECK (step IN ('verify_org', 'create_database',
                                                     'insert_routing', 'migrate',
                                                     'contract', 'fingerprint',
                                                     'provision_dek', 'activate',
                                                     'done', 'cleanup')),
    status               TEXT        NOT NULL DEFAULT 'provisioning'
                                     CHECK (status IN ('provisioning', 'ready', 'failed')),
    attempts             INTEGER     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    -- Human-readable. Never a DSN, never a key.
    last_error           TEXT,
    next_attempt_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Lease: a machine claims the job by writing its own id and an expiry;
    -- every later write by that machine is conditioned on still holding it.
    lease_owner          TEXT,
    lease_expires_at     TIMESTAMPTZ,
    -- What THIS job created, recorded the moment it is known and BEFORE
    -- anything else uses it. Cleanup deletes exactly these, in reverse order.
    provider             TEXT        CHECK (provider IN ('neon', 'local')),
    -- A Neon project id (`[a-z0-9-]`) or a local database name
    -- (`[a-z0-9_]`). Cleanup deletes by this id, so the shape is pinned.
    provider_database_id TEXT        CHECK (provider_database_id ~ '^[a-z0-9_-]{1,63}$'),
    -- Recorded BEFORE the job asks the provider to create anything.
    -- `create_database` looks a database up by name first (a crash between
    -- create and record must not mint a second Neon project); a database found
    -- that way is taken only when this intent is already on the row, so every
    -- recorded `provider_database_id` was requested by THIS job and cleanup
    -- never deletes a concierge tenant mid-onboarding or another org's.
    database_create_requested_at TIMESTAMPTZ,
    credential_stored_at TIMESTAMPTZ,
    routing_inserted_at  TIMESTAMPTZ,
    -- Set by `cancel`: the worker turns it into a cleanup on its next pass.
    cancel_requested_at  TIMESTAMPTZ,
    -- When the job started waiting for the org's subscription to go live
    -- (async payment methods such as ACH / SEPA can take days). That wait
    -- has its own deadline (`RetryPolicy::subscription_wait`) and does not
    -- spend the transient-error attempts budget. Cleared on every step change.
    subscription_wait_started_at TIMESTAMPTZ,
    -- When `activate` first refused because a newer tenant tree is already
    -- rolled out (55000: this build is behind head). Like the subscription
    -- wait, a park, not an error: it never spends the attempts budget and
    -- never leads to cleanup (`RetryPolicy::version_wait` only decides when
    -- to alert). Cleared on every step change.
    version_wait_started_at TIMESTAMPTZ,
    -- Terminal-failure hook for PR A2 (which creates the Clerk org and owns
    -- its deletion): 'pending' once a job has failed terminally and cleaned
    -- up, so A2 can delete or mark the orphaned Clerk org; 'none' for a job
    -- that never failed; 'done' once A2 has acted. PR A1 never calls Clerk.
    clerk_org_cleanup    TEXT        NOT NULL DEFAULT 'none'
                                     CHECK (clerk_org_cleanup IN ('none', 'pending', 'done')),
    failed_at            TIMESTAMPTZ,
    ready_at             TIMESTAMPTZ,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT tenant_provisioning_jobs_account_key UNIQUE (account_id),
    CONSTRAINT tenant_provisioning_jobs_secret_ref_key UNIQUE (db_secret_ref),
    -- A ready job is always on `done`; a live job never is.
    CONSTRAINT tenant_provisioning_jobs_ready_is_done
        CHECK ((status = 'ready') = (step = 'done')),
    CONSTRAINT tenant_provisioning_jobs_recorded_database_was_requested
        CHECK (provider_database_id IS NULL OR database_create_requested_at IS NOT NULL),
    CONSTRAINT tenant_provisioning_jobs_slug_is_derived
        CHECK (slug = 'o-' || left(replace(account_id::text, '-', ''), 16)),
    CONSTRAINT tenant_provisioning_jobs_secret_ref_is_derived
        CHECK (db_secret_ref = 'TENANT_DB_' || upper(replace(slug, '-', '_')))
);

-- The worker's claim query: live jobs whose next attempt is due.
CREATE INDEX tenant_provisioning_jobs_due_idx
    ON tenant_provisioning_jobs (next_attempt_at)
    WHERE status = 'provisioning';

-- ── 3. SECURITY DEFINER carve-outs ───────────────────────────────────────────
--
-- Shape follows 0084/0086 (`SECURITY DEFINER`, pinned `search_path`). Owned
-- by the migration runner (the `public.tenant_databases` owner — 0321 asserts the
-- app roles can never assume that owner), EXECUTE revoked from PUBLIC. The
-- worker's definers (3a-3f, 3h-3o) are granted to `coralswarm_provisioner`
-- only; `coralswarm_app` gets enqueue, cancel, mark-cleanup-done and retry
-- (3g, 3p-3r). Each checks its own invariants and raises on violation; none
-- returns a silent no-op where the caller would read "done".

-- Lease-conditioned job writes (3h-3o) match on `lease_owner = p_owner AND
-- status = 'provisioning'` and return false when the caller no longer holds
-- the lease. Every write that keeps the lease also RENEWS it
-- (`lease_expires_at = now() + p_ttl`), so a long job never runs past its
-- own expiry while another machine re-claims it.

-- 3a. Store a credential. Insert-if-absent and NEVER overwrite: an existing
-- ref already names a database somebody may be serving, and re-pointing it
-- is exactly the write this hardening exists to deny. Only for a ref a live
-- job owns and has already recorded its database for, so the app role
-- cannot plant a credential under an arbitrary ref. Returns true when the
-- row was inserted, false when an identical ciphertext was already present
-- (a retried step converging); raises when a DIFFERENT ciphertext is present
-- (two jobs, or a replay with a different DSN — never silently merged).
CREATE FUNCTION tenant_provisioning_store_credential(p_secret_ref text, p_dsn_encrypted bytea)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    existing bytea;
BEGIN
    IF p_dsn_encrypted IS NULL OR length(p_dsn_encrypted) < 29 THEN
        RAISE EXCEPTION 'tenant credential ciphertext is malformed' USING ERRCODE = '22023';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM public.tenant_provisioning_jobs
                    WHERE db_secret_ref = p_secret_ref AND status = 'provisioning'
                      AND provider_database_id IS NOT NULL) THEN
        RAISE EXCEPTION 'no live provisioning job with a recorded database owns credential %', p_secret_ref
            USING ERRCODE = 'P0002';
    END IF;
    SELECT dsn_encrypted INTO existing FROM public.tenant_db_credentials WHERE secret_ref = p_secret_ref;
    IF FOUND THEN
        IF existing = p_dsn_encrypted THEN
            RETURN false;
        END IF;
        RAISE EXCEPTION 'tenant credential % already exists with different contents; refusing to overwrite', p_secret_ref
            USING ERRCODE = '23505';
    END IF;
    INSERT INTO public.tenant_db_credentials (secret_ref, dsn_encrypted) VALUES (p_secret_ref, p_dsn_encrypted);
    RETURN true;
END
$$;

-- 3b. Delete a credential — only one no routing row references. Returns the
-- number of rows deleted (0 when already gone: cleanup converges).
CREATE FUNCTION tenant_provisioning_delete_credential(p_secret_ref text)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    n integer;
BEGIN
    IF EXISTS (SELECT 1 FROM public.tenant_databases WHERE db_secret_ref = p_secret_ref) THEN
        RAISE EXCEPTION 'tenant credential % is still referenced by a routing row', p_secret_ref
            USING ERRCODE = '23503';
    END IF;
    DELETE FROM public.tenant_db_credentials WHERE secret_ref = p_secret_ref;
    GET DIAGNOSTICS n = ROW_COUNT;
    RETURN n;
END
$$;

-- 3c. Insert the routing row as `provisioning`. The job row is NOT trusted
-- on its own: the function re-derives the self-serve shape from the account
-- id (slug `o-` + 16 hex, ref `TENANT_DB_` + that slug upper-cased) and
-- requires, besides a live job on this step whose recorded binding matches:
-- the database already recorded and its credential stored (so a release's
-- `migrate_tenants` can always resolve this row); the org `accounts` row
-- 0321's trigger will check; no personal org; no routing row for this org
-- (or for this slug/ref under another org); and a LIVE subscription
-- (`stripe_subscriptions` active with a Stripe subscription id). Enters the
-- account for the trigger. Idempotent: an existing `provisioning` row with
-- the same binding returns false; any other existing row raises (never
-- clobber a live tenant).
CREATE FUNCTION tenant_provisioning_insert_routing(
    p_clerk_org_id text, p_account_id uuid, p_slug text, p_secret_ref text, p_region text)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    job public.tenant_provisioning_jobs;
    existing public.tenant_databases;
    v_slug text;
    v_ref text;
BEGIN
    SELECT * INTO job FROM public.tenant_provisioning_jobs WHERE clerk_org_id = p_clerk_org_id FOR UPDATE;
    IF NOT FOUND OR job.status <> 'provisioning' THEN
        RAISE EXCEPTION 'no live provisioning job for org %', p_clerk_org_id USING ERRCODE = 'P0002';
    END IF;
    IF job.account_id <> p_account_id OR job.slug <> p_slug OR job.db_secret_ref <> p_secret_ref THEN
        RAISE EXCEPTION 'routing insert for org % does not match its provisioning job', p_clerk_org_id
            USING ERRCODE = '23514';
    END IF;
    v_slug := 'o-' || left(replace(p_account_id::text, '-', ''), 16);
    v_ref := 'TENANT_DB_' || upper(replace(v_slug, '-', '_'));
    IF p_slug <> v_slug OR p_secret_ref <> v_ref THEN
        RAISE EXCEPTION 'routing insert for org % is not the self-serve shape (slug %, ref % expected)',
            p_clerk_org_id, v_slug, v_ref USING ERRCODE = '23514';
    END IF;

    SELECT * INTO existing FROM public.tenant_databases WHERE clerk_org_id = p_clerk_org_id FOR UPDATE;
    IF FOUND THEN
        IF existing.status = 'provisioning' AND existing.account_id = p_account_id
           AND existing.slug = p_slug AND existing.db_secret_ref = p_secret_ref
           AND job.routing_inserted_at IS NOT NULL THEN
            RETURN false;
        END IF;
        RAISE EXCEPTION 'org % already has a routing row in status %; refusing to re-provision',
            p_clerk_org_id, existing.status USING ERRCODE = '23505';
    END IF;

    IF job.step <> 'insert_routing' THEN
        RAISE EXCEPTION 'provisioning job for org % is on step %, not insert_routing', p_clerk_org_id, job.step
            USING ERRCODE = '23514';
    END IF;
    IF job.provider_database_id IS NULL OR job.database_create_requested_at IS NULL THEN
        RAISE EXCEPTION 'routing insert for org % before its database was recorded', p_clerk_org_id
            USING ERRCODE = '23514';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM public.tenant_db_credentials WHERE secret_ref = p_secret_ref) THEN
        RAISE EXCEPTION 'routing insert for org % before its credential was stored', p_clerk_org_id
            USING ERRCODE = '23514';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM public.accounts WHERE id = p_account_id AND clerk_org_id = p_clerk_org_id AND is_org_account) THEN
        RAISE EXCEPTION 'org % has no control accounts row bound to account %', p_clerk_org_id, p_account_id
            USING ERRCODE = '23503';
    END IF;
    IF EXISTS (SELECT 1 FROM public.personal_orgs WHERE clerk_org_id = p_clerk_org_id) THEN
        RAISE EXCEPTION 'org % is a personal organization; it gets no tenant database', p_clerk_org_id
            USING ERRCODE = '23514';
    END IF;
    IF EXISTS (SELECT 1 FROM public.tenant_databases WHERE slug = p_slug OR db_secret_ref = p_secret_ref) THEN
        RAISE EXCEPTION 'slug % or ref % is already routed to another org', p_slug, p_secret_ref
            USING ERRCODE = '23505';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM public.stripe_subscriptions
                    WHERE account_id = p_account_id AND status = 'active'
                      AND stripe_subscription_id IS NOT NULL) THEN
        RAISE EXCEPTION 'org % has no live subscription; no routing row is created', p_clerk_org_id
            USING ERRCODE = '23514';
    END IF;

    PERFORM set_config('app.account_id', p_account_id::text, true);
    PERFORM public.billing_enter_account(p_account_id);
    INSERT INTO public.tenant_databases (clerk_org_id, account_id, slug, db_secret_ref, region, status)
    VALUES (p_clerk_org_id, p_account_id, p_slug, p_secret_ref, p_region, 'provisioning');
    UPDATE public.tenant_provisioning_jobs
       SET routing_inserted_at = COALESCE(routing_inserted_at, now()), updated_at = now()
     WHERE clerk_org_id = p_clerk_org_id;
    RETURN true;
END
$$;

-- 3d. Record the applied tenant-tree version onto a routing row that is
-- still being provisioned. Returns the stored value so the caller reads back
-- what landed instead of trusting rows_affected; a missing routing row
-- raises (never "recorded" against nothing — audit 4d).
--
-- Narrow on purpose: only by the lease holder of a live job on `migrate`
-- (the job's in-process `migrate_all` is the only caller), only onto that
-- job's `provisioning` row, never DOWNWARD, and never above 9999 (tenant
-- migration files are numbered with four digits, `scripts/check_migrations.sh`;
-- a larger value is not a version any build has). Renews the lease. Every
-- other tenant's version is written by the release's owner-role
-- `migrate_tenants`, never through here.
CREATE FUNCTION tenant_provisioning_record_schema_version(
    p_clerk_org_id text, p_owner text, p_ttl interval, p_version bigint)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    job public.tenant_provisioning_jobs;
    row public.tenant_databases;
    stored bigint;
BEGIN
    IF p_version IS NULL OR p_version < 0 OR p_version > 9999 THEN
        RAISE EXCEPTION 'schema version % is not a tenant migration version (0-9999)', p_version
            USING ERRCODE = '22023';
    END IF;
    IF p_ttl IS NULL OR p_ttl < interval '1 second' OR p_ttl > interval '6 hours' THEN
        RAISE EXCEPTION 'lease ttl % is out of range', p_ttl USING ERRCODE = '22023';
    END IF;
    SELECT * INTO job FROM public.tenant_provisioning_jobs WHERE clerk_org_id = p_clerk_org_id FOR UPDATE;
    IF NOT FOUND OR job.status <> 'provisioning' THEN
        RAISE EXCEPTION 'routing row for org % has no live provisioning job', p_clerk_org_id
            USING ERRCODE = '23514';
    END IF;
    IF p_owner IS NULL OR job.lease_owner IS DISTINCT FROM p_owner THEN
        RAISE EXCEPTION 'caller does not hold the provisioning lease for org %', p_clerk_org_id
            USING ERRCODE = '23514';
    END IF;
    IF job.step <> 'migrate' THEN
        RAISE EXCEPTION 'provisioning job for org % is on step %, not migrate', p_clerk_org_id, job.step
            USING ERRCODE = '23514';
    END IF;
    SELECT * INTO row FROM public.tenant_databases WHERE clerk_org_id = p_clerk_org_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'no routing row for org %; schema version not recorded', p_clerk_org_id
            USING ERRCODE = 'P0002';
    END IF;
    IF row.status <> 'provisioning' THEN
        RAISE EXCEPTION 'routing row for org % is %; only a row being provisioned records its version here',
            p_clerk_org_id, row.status USING ERRCODE = '23514';
    END IF;
    IF row.account_id <> job.account_id OR row.db_secret_ref <> job.db_secret_ref THEN
        RAISE EXCEPTION 'routing row for org % is not the one its job created', p_clerk_org_id
            USING ERRCODE = '23514';
    END IF;
    IF row.schema_version IS NOT NULL AND p_version < row.schema_version THEN
        RAISE EXCEPTION 'routing row for org % is at schema_version %; refusing to lower it to %',
            p_clerk_org_id, row.schema_version, p_version USING ERRCODE = '23514';
    END IF;
    UPDATE public.tenant_databases
       SET schema_version = p_version, updated_at = now()
     WHERE clerk_org_id = p_clerk_org_id
    RETURNING schema_version INTO stored;
    UPDATE public.tenant_provisioning_jobs
       SET lease_expires_at = now() + p_ttl, updated_at = now()
     WHERE clerk_org_id = p_clerk_org_id;
    RETURN stored;
END
$$;

-- 3e. Flip `provisioning → active` and the job to `ready`, atomically.
-- Requires the job on `activate`, the routing row `provisioning`, and its
-- schema_version equal to the version the caller's binary expects (the
-- worker harness skips — and the request path 503s — any tenant below head,
-- so activating a behind-head tenant would hand the org a 503).
--
-- Rolling deploys: an OLD build's worker could otherwise activate at the old
-- head after the new release's `migrate_tenants` already ran. So activation
-- also refuses while any other ACTIVE routing row records a NEWER version
-- than the caller expects (a newer tree has been rolled out; the new build's
-- worker re-migrates and activates instead). Only active rows count: they
-- are the ones the release migrates, so their versions are a release's
-- word, while a `provisioning` row's version is just another job's. Those
-- rows are read FOR SHARE, so a concurrent release write either lands first
-- (and is seen) or waits for this activation to commit (and the release's
-- catch-up pass then migrates the newly active row). Raised as 55000, which
-- the job treats as a park (it waits for the newer build without spending
-- its attempts budget), never as a failure.
-- Converges: an already-ready job whose row is already active returns false.
CREATE FUNCTION tenant_provisioning_activate(p_clerk_org_id text, p_expected_version bigint)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    job public.tenant_provisioning_jobs;
    row public.tenant_databases;
    newest bigint;
BEGIN
    SELECT * INTO job FROM public.tenant_provisioning_jobs WHERE clerk_org_id = p_clerk_org_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'no provisioning job for org %', p_clerk_org_id USING ERRCODE = 'P0002';
    END IF;
    SELECT * INTO row FROM public.tenant_databases WHERE clerk_org_id = p_clerk_org_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'no routing row for org %; cannot activate', p_clerk_org_id USING ERRCODE = 'P0002';
    END IF;
    IF job.status = 'ready' AND row.status = 'active' THEN
        RETURN false;
    END IF;
    IF job.status <> 'provisioning' THEN
        RAISE EXCEPTION 'provisioning job for org % is %, not live', p_clerk_org_id, job.status
            USING ERRCODE = '23514';
    END IF;
    IF job.step <> 'activate' THEN
        RAISE EXCEPTION 'provisioning job for org % is on step %, not activate', p_clerk_org_id, job.step
            USING ERRCODE = '23514';
    END IF;
    IF row.status <> 'provisioning' THEN
        RAISE EXCEPTION 'routing row for org % is %, expected provisioning', p_clerk_org_id, row.status
            USING ERRCODE = '23514';
    END IF;
    IF row.schema_version IS DISTINCT FROM p_expected_version THEN
        RAISE EXCEPTION 'routing row for org % has schema_version % but this build expects %; not activating',
            p_clerk_org_id, row.schema_version, p_expected_version USING ERRCODE = '23514';
    END IF;
    SELECT max(t.schema_version) INTO newest
      FROM (SELECT schema_version FROM public.tenant_databases
             WHERE clerk_org_id <> p_clerk_org_id AND status = 'active'
             FOR SHARE) t;
    IF newest IS NOT NULL AND newest > p_expected_version THEN
        RAISE EXCEPTION 'a newer tenant tree (v%) is already rolled out; this build (v%) must not activate org %',
            newest, p_expected_version, p_clerk_org_id USING ERRCODE = '55000';
    END IF;
    UPDATE public.tenant_databases SET status = 'active', updated_at = now() WHERE clerk_org_id = p_clerk_org_id;
    UPDATE public.tenant_provisioning_jobs
       SET status = 'ready', step = 'done', ready_at = now(), last_error = NULL,
           lease_owner = NULL, lease_expires_at = NULL, subscription_wait_started_at = NULL,
           version_wait_started_at = NULL, updated_at = now()
     WHERE clerk_org_id = p_clerk_org_id;
    RETURN true;
END
$$;

-- 3f. Delete a `provisioning` routing row during cleanup — never an active,
-- suspended or offboarded one (those are live or audit rows). Enters the
-- account for 0321's DELETE check. Returns rows deleted (0 = already gone).
CREATE FUNCTION tenant_provisioning_delete_routing(p_clerk_org_id text)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    job public.tenant_provisioning_jobs;
    row public.tenant_databases;
    n integer;
BEGIN
    SELECT * INTO job FROM public.tenant_provisioning_jobs WHERE clerk_org_id = p_clerk_org_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'no provisioning job for org %', p_clerk_org_id USING ERRCODE = 'P0002';
    END IF;
    IF job.status = 'ready' THEN
        RAISE EXCEPTION 'provisioning job for org % is ready; its routing row is live', p_clerk_org_id
            USING ERRCODE = '23514';
    END IF;
    SELECT * INTO row FROM public.tenant_databases WHERE clerk_org_id = p_clerk_org_id FOR UPDATE;
    IF NOT FOUND THEN
        RETURN 0;
    END IF;
    IF row.status <> 'provisioning' THEN
        RAISE EXCEPTION 'routing row for org % is %, refusing to delete', p_clerk_org_id, row.status
            USING ERRCODE = '23514';
    END IF;
    IF row.account_id <> job.account_id THEN
        RAISE EXCEPTION 'routing row for org % is bound to a different account than its job', p_clerk_org_id
            USING ERRCODE = '23514';
    END IF;
    PERFORM set_config('app.account_id', row.account_id::text, true);
    PERFORM public.billing_enter_account(row.account_id);
    DELETE FROM public.tenant_databases WHERE clerk_org_id = p_clerk_org_id;
    GET DIAGNOSTICS n = ROW_COUNT;
    RETURN n;
END
$$;

-- 3g. Queue a job. The slug and ref are DERIVED here from the account id,
-- never taken from the caller, and the preconditions `tenancy::provisioning::
-- enqueue_in` checks with typed errors are re-checked: not a personal org,
-- an org `accounts` row bound to this account, no routing row for the org
-- or for the derived slug/ref. Returns true when inserted, false when a job
-- for this org already exists (a redelivered webhook). A different org
-- already holding this account's job raises 23505 (UNIQUE account_id).
CREATE FUNCTION tenant_provisioning_enqueue(p_clerk_org_id text, p_account_id uuid, p_region text)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    v_slug text;
    v_ref text;
BEGIN
    IF p_clerk_org_id IS NULL OR p_clerk_org_id = '' OR p_account_id IS NULL THEN
        RAISE EXCEPTION 'enqueue needs an org id and an account id' USING ERRCODE = '22023';
    END IF;
    IF p_region IS NOT NULL AND p_region !~ '^[a-z0-9-]{1,63}$' THEN
        RAISE EXCEPTION 'region % is not a provider region id', p_region USING ERRCODE = '22023';
    END IF;
    IF EXISTS (SELECT 1 FROM public.personal_orgs WHERE clerk_org_id = p_clerk_org_id) THEN
        RAISE EXCEPTION 'org % is a personal organization; it gets no tenant database', p_clerk_org_id
            USING ERRCODE = '23514';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM public.accounts WHERE id = p_account_id AND clerk_org_id = p_clerk_org_id AND is_org_account) THEN
        RAISE EXCEPTION 'org % has no control accounts row bound to account %', p_clerk_org_id, p_account_id
            USING ERRCODE = '23503';
    END IF;
    v_slug := 'o-' || left(replace(p_account_id::text, '-', ''), 16);
    v_ref := 'TENANT_DB_' || upper(replace(v_slug, '-', '_'));
    IF EXISTS (SELECT 1 FROM public.tenant_databases
                WHERE clerk_org_id = p_clerk_org_id OR slug = v_slug OR db_secret_ref = v_ref) THEN
        RAISE EXCEPTION 'org % (or its routing key %) is already routed', p_clerk_org_id, v_slug
            USING ERRCODE = '23505';
    END IF;
    INSERT INTO public.tenant_provisioning_jobs (clerk_org_id, account_id, slug, db_secret_ref, region)
    VALUES (p_clerk_org_id, p_account_id, v_slug, v_ref, p_region)
    ON CONFLICT (clerk_org_id) DO NOTHING;
    RETURN FOUND;
END
$$;

-- 3h. Claim a job: the named org's (an operator retry, tests) when
-- `p_clerk_org_id` is given, else the earliest due one under SKIP LOCKED.
-- A held, unexpired lease is only re-claimable by its own owner.
CREATE FUNCTION tenant_provisioning_claim(p_owner text, p_ttl interval, p_clerk_org_id text)
RETURNS SETOF public.tenant_provisioning_jobs
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
BEGIN
    IF p_owner IS NULL OR p_owner = '' THEN
        RAISE EXCEPTION 'lease owner must be named' USING ERRCODE = '22023';
    END IF;
    IF p_ttl IS NULL OR p_ttl < interval '1 second' OR p_ttl > interval '6 hours' THEN
        RAISE EXCEPTION 'lease ttl % is out of range', p_ttl USING ERRCODE = '22023';
    END IF;
    IF p_clerk_org_id IS NULL THEN
        RETURN QUERY
        UPDATE public.tenant_provisioning_jobs j
           SET lease_owner = p_owner, lease_expires_at = now() + p_ttl, updated_at = now()
         WHERE j.clerk_org_id = (
               SELECT q.clerk_org_id FROM public.tenant_provisioning_jobs q
                WHERE q.status = 'provisioning' AND q.next_attempt_at <= now()
                  AND (q.lease_expires_at IS NULL OR q.lease_expires_at < now())
                ORDER BY q.next_attempt_at
                FOR UPDATE SKIP LOCKED LIMIT 1)
        RETURNING j.*;
    ELSE
        RETURN QUERY
        UPDATE public.tenant_provisioning_jobs j
           SET lease_owner = p_owner, lease_expires_at = now() + p_ttl, updated_at = now()
         WHERE j.clerk_org_id = p_clerk_org_id AND j.status = 'provisioning'
           AND (j.lease_expires_at IS NULL OR j.lease_expires_at < now() OR j.lease_owner = p_owner)
        RETURNING j.*;
    END IF;
END
$$;

-- 3i. Move a held job to its next step, renewing the lease. Only the
-- forward path (each step to its successor), `activate → migrate` (a newer
-- build re-migrating), or any live step to `cleanup`. Resets the attempts
-- budget and both waits; `p_last_error` is the cleanup reason (NULL on a
-- forward move).
CREATE FUNCTION tenant_provisioning_set_step(
    p_clerk_org_id text, p_owner text, p_ttl interval, p_step text, p_last_error text)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    job public.tenant_provisioning_jobs;
    successor text;
BEGIN
    IF p_ttl IS NULL OR p_ttl < interval '1 second' OR p_ttl > interval '6 hours' THEN
        RAISE EXCEPTION 'lease ttl % is out of range', p_ttl USING ERRCODE = '22023';
    END IF;
    SELECT * INTO job FROM public.tenant_provisioning_jobs WHERE clerk_org_id = p_clerk_org_id FOR UPDATE;
    IF NOT FOUND OR job.status <> 'provisioning' OR job.lease_owner IS DISTINCT FROM p_owner THEN
        RETURN false;
    END IF;
    successor := CASE job.step
        WHEN 'verify_org' THEN 'create_database'
        WHEN 'create_database' THEN 'insert_routing'
        WHEN 'insert_routing' THEN 'migrate'
        WHEN 'migrate' THEN 'contract'
        WHEN 'contract' THEN 'fingerprint'
        WHEN 'fingerprint' THEN 'provision_dek'
        WHEN 'provision_dek' THEN 'activate'
    END;
    IF NOT (p_step IS NOT DISTINCT FROM successor
            OR (p_step = 'cleanup' AND job.step <> 'done')
            OR (job.step = 'activate' AND p_step = 'migrate')) THEN
        RAISE EXCEPTION 'provisioning job for org % cannot move from % to %', p_clerk_org_id, job.step, p_step
            USING ERRCODE = '23514';
    END IF;
    UPDATE public.tenant_provisioning_jobs
       SET step = p_step, attempts = 0, last_error = p_last_error,
           subscription_wait_started_at = NULL, version_wait_started_at = NULL,
           lease_expires_at = now() + p_ttl, updated_at = now()
     WHERE clerk_org_id = p_clerk_org_id;
    RETURN true;
END
$$;

-- 3j. Defer a held job and release the lease. `p_last_error` NULL keeps the
-- recorded reason (a cleanup retry keeps its terminal reason). `p_wait`
-- names a park that runs on its own clock rather than the attempts budget:
-- 'subscription' (the org's subscription is not live yet) or 'version'
-- (activation refused behind a newer rolled-out tree). It starts that
-- clock if it is not already running; NULL is a plain transient retry.
CREATE FUNCTION tenant_provisioning_defer(
    p_clerk_org_id text, p_owner text, p_attempts integer, p_last_error text,
    p_delay interval, p_wait text)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
BEGIN
    IF p_attempts IS NULL OR p_attempts < 0 THEN
        RAISE EXCEPTION 'attempts must be non-negative' USING ERRCODE = '22023';
    END IF;
    IF p_delay IS NULL OR p_delay < interval '0' OR p_delay > interval '7 days' THEN
        RAISE EXCEPTION 'retry delay % is out of range', p_delay USING ERRCODE = '22023';
    END IF;
    IF p_wait IS NOT NULL AND p_wait NOT IN ('subscription', 'version') THEN
        RAISE EXCEPTION 'unknown wait %', p_wait USING ERRCODE = '22023';
    END IF;
    UPDATE public.tenant_provisioning_jobs
       SET attempts = p_attempts,
           last_error = COALESCE(p_last_error, last_error),
           next_attempt_at = now() + p_delay,
           subscription_wait_started_at = CASE WHEN p_wait = 'subscription'
                                               THEN COALESCE(subscription_wait_started_at, now())
                                               ELSE subscription_wait_started_at END,
           version_wait_started_at = CASE WHEN p_wait = 'version'
                                          THEN COALESCE(version_wait_started_at, now())
                                          ELSE version_wait_started_at END,
           lease_owner = NULL, lease_expires_at = NULL, updated_at = now()
     WHERE clerk_org_id = p_clerk_org_id AND lease_owner = p_owner AND status = 'provisioning';
    RETURN FOUND;
END
$$;

-- 3k. Record the intent to create a database, BEFORE asking the provider.
CREATE FUNCTION tenant_provisioning_record_create_intent(p_clerk_org_id text, p_owner text, p_ttl interval)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
BEGIN
    IF p_ttl IS NULL OR p_ttl < interval '1 second' OR p_ttl > interval '6 hours' THEN
        RAISE EXCEPTION 'lease ttl % is out of range', p_ttl USING ERRCODE = '22023';
    END IF;
    UPDATE public.tenant_provisioning_jobs
       SET database_create_requested_at = COALESCE(database_create_requested_at, now()),
           lease_expires_at = now() + p_ttl, updated_at = now()
     WHERE clerk_org_id = p_clerk_org_id AND lease_owner = p_owner AND status = 'provisioning'
       AND step = 'create_database';
    RETURN FOUND;
END
$$;

-- 3l. Drop the create intent after the provider said the name was taken by
-- something else. A recorded id keeps its intent (the next attempt refuses
-- on the id mismatch instead).
CREATE FUNCTION tenant_provisioning_clear_create_intent(p_clerk_org_id text, p_owner text, p_ttl interval)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
BEGIN
    IF p_ttl IS NULL OR p_ttl < interval '1 second' OR p_ttl > interval '6 hours' THEN
        RAISE EXCEPTION 'lease ttl % is out of range', p_ttl USING ERRCODE = '22023';
    END IF;
    UPDATE public.tenant_provisioning_jobs
       SET database_create_requested_at = CASE WHEN provider_database_id IS NULL
                                               THEN NULL ELSE database_create_requested_at END,
           lease_expires_at = now() + p_ttl, updated_at = now()
     WHERE clerk_org_id = p_clerk_org_id AND lease_owner = p_owner AND status = 'provisioning'
       AND step = 'create_database';
    RETURN FOUND;
END
$$;

-- 3m. Record the provider database this job created. Once: a different id
-- (or provider) over an already recorded one is refused, never replaced.
-- Requires the create intent (the table CHECK backs that) and pins the id
-- shape (column CHECK), because cleanup deletes by it.
CREATE FUNCTION tenant_provisioning_record_database(
    p_clerk_org_id text, p_owner text, p_ttl interval, p_provider text, p_provider_database_id text)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    job public.tenant_provisioning_jobs;
BEGIN
    IF p_ttl IS NULL OR p_ttl < interval '1 second' OR p_ttl > interval '6 hours' THEN
        RAISE EXCEPTION 'lease ttl % is out of range', p_ttl USING ERRCODE = '22023';
    END IF;
    SELECT * INTO job FROM public.tenant_provisioning_jobs WHERE clerk_org_id = p_clerk_org_id FOR UPDATE;
    IF NOT FOUND OR job.status <> 'provisioning' OR job.lease_owner IS DISTINCT FROM p_owner THEN
        RETURN false;
    END IF;
    IF job.step <> 'create_database' THEN
        RAISE EXCEPTION 'provisioning job for org % is on step %, not create_database', p_clerk_org_id, job.step
            USING ERRCODE = '23514';
    END IF;
    IF job.database_create_requested_at IS NULL THEN
        RAISE EXCEPTION 'provisioning job for org % never asked to create a database', p_clerk_org_id
            USING ERRCODE = '23514';
    END IF;
    IF job.provider_database_id IS NOT NULL
       AND (job.provider_database_id <> p_provider_database_id OR job.provider IS DISTINCT FROM p_provider) THEN
        RAISE EXCEPTION 'provisioning job for org % already recorded database %; refusing to replace it',
            p_clerk_org_id, job.provider_database_id USING ERRCODE = '23514';
    END IF;
    UPDATE public.tenant_provisioning_jobs
       SET provider = p_provider, provider_database_id = p_provider_database_id,
           lease_expires_at = now() + p_ttl, updated_at = now()
     WHERE clerk_org_id = p_clerk_org_id;
    RETURN true;
END
$$;

-- 3n. Record that this job's credential is stored (it must be).
CREATE FUNCTION tenant_provisioning_record_credential_stored(p_clerk_org_id text, p_owner text, p_ttl interval)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    job public.tenant_provisioning_jobs;
BEGIN
    IF p_ttl IS NULL OR p_ttl < interval '1 second' OR p_ttl > interval '6 hours' THEN
        RAISE EXCEPTION 'lease ttl % is out of range', p_ttl USING ERRCODE = '22023';
    END IF;
    SELECT * INTO job FROM public.tenant_provisioning_jobs WHERE clerk_org_id = p_clerk_org_id FOR UPDATE;
    IF NOT FOUND OR job.status <> 'provisioning' OR job.lease_owner IS DISTINCT FROM p_owner THEN
        RETURN false;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM public.tenant_db_credentials WHERE secret_ref = job.db_secret_ref) THEN
        RAISE EXCEPTION 'credential % is not stored', job.db_secret_ref USING ERRCODE = '23514';
    END IF;
    UPDATE public.tenant_provisioning_jobs
       SET credential_stored_at = COALESCE(credential_stored_at, now()),
           lease_expires_at = now() + p_ttl, updated_at = now()
     WHERE clerk_org_id = p_clerk_org_id;
    RETURN true;
END
$$;

-- 3o. Mark a held job, on `cleanup`, terminally failed and release it.
-- `p_forget_database` (its own database is gone) clears the provider id AND
-- the create intent, so a later retry never adopts a same-name database
-- someone else creates for the one this job once asked for.
CREATE FUNCTION tenant_provisioning_fail(
    p_clerk_org_id text, p_owner text, p_last_error text, p_forget_database boolean)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
BEGIN
    UPDATE public.tenant_provisioning_jobs
       SET status = 'failed', last_error = p_last_error, failed_at = now(),
           clerk_org_cleanup = 'pending', lease_owner = NULL, lease_expires_at = NULL,
           provider_database_id = CASE WHEN p_forget_database THEN NULL ELSE provider_database_id END,
           database_create_requested_at = CASE WHEN p_forget_database THEN NULL
                                               ELSE database_create_requested_at END,
           updated_at = now()
     WHERE clerk_org_id = p_clerk_org_id AND lease_owner = p_owner AND status = 'provisioning'
       AND step = 'cleanup';
    RETURN FOUND;
END
$$;

-- 3p. Ask the worker to abandon a live job (it cleans up on its next pass).
CREATE FUNCTION tenant_provisioning_cancel(p_clerk_org_id text)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
BEGIN
    UPDATE public.tenant_provisioning_jobs
       SET cancel_requested_at = COALESCE(cancel_requested_at, now()),
           next_attempt_at = now(), updated_at = now()
     WHERE clerk_org_id = p_clerk_org_id AND status = 'provisioning';
    RETURN FOUND;
END
$$;

-- 3q. PR A2 records that it has acted on a failed job's orphaned Clerk org.
CREATE FUNCTION tenant_provisioning_mark_clerk_org_cleanup_done(p_clerk_org_id text)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
BEGIN
    UPDATE public.tenant_provisioning_jobs
       SET clerk_org_cleanup = 'done', updated_at = now()
     WHERE clerk_org_id = p_clerk_org_id AND clerk_org_cleanup = 'pending';
    RETURN FOUND;
END
$$;

-- 3r. Retry a terminally failed job (PR A2's `POST …/provisioning/retry`,
-- which runs as the app role). Only `failed → verify_org`: the attempts
-- budget, both waits, the cancel request, the lease and the failure are
-- cleared, and the job runs from the top on the worker's next poll (every
-- step re-checks what it needs, the subscription included). Refuses
-- anything else: no job (P0002); a job that is not `failed` (23514); a job
-- whose orphaned Clerk org A2 has already dealt with (`clerk_org_cleanup =
-- 'done'`, 23514); an org that has a routing row in any status other than
-- `provisioning` (23505). What the failed run recorded creating is kept,
-- so a retry resumes on it rather than leaking it.
CREATE FUNCTION tenant_provisioning_retry(p_clerk_org_id text)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    job public.tenant_provisioning_jobs;
    routed text;
BEGIN
    SELECT * INTO job FROM public.tenant_provisioning_jobs WHERE clerk_org_id = p_clerk_org_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'no provisioning job for org %', p_clerk_org_id USING ERRCODE = 'P0002';
    END IF;
    IF job.status <> 'failed' THEN
        RAISE EXCEPTION 'provisioning job for org % is %; only a failed job is retried', p_clerk_org_id, job.status
            USING ERRCODE = '23514';
    END IF;
    IF job.clerk_org_cleanup = 'done' THEN
        RAISE EXCEPTION 'the Clerk org for failed job % was already cleaned up; it cannot be retried', p_clerk_org_id
            USING ERRCODE = '23514';
    END IF;
    SELECT status INTO routed FROM public.tenant_databases WHERE clerk_org_id = p_clerk_org_id;
    IF FOUND AND routed <> 'provisioning' THEN
        RAISE EXCEPTION 'org % has a routing row in status %; refusing to re-provision', p_clerk_org_id, routed
            USING ERRCODE = '23505';
    END IF;
    UPDATE public.tenant_provisioning_jobs
       SET status = 'provisioning', step = 'verify_org', attempts = 0, last_error = NULL,
           next_attempt_at = now(), failed_at = NULL, clerk_org_cleanup = 'none',
           cancel_requested_at = NULL, subscription_wait_started_at = NULL,
           version_wait_started_at = NULL, lease_owner = NULL, lease_expires_at = NULL,
           updated_at = now()
     WHERE clerk_org_id = p_clerk_org_id;
    RETURN true;
END
$$;

-- ── Roles and privileges ──────────────────────────────────────────────────────
--
-- `coralswarm_provisioner`: a NOLOGIN group role holding EXECUTE on the
-- worker's definers and SELECT on the three tables they keep. The operator
-- creates a separate LOGIN role, grants it this one, and gives its DSN to
-- the worker as `PROVISIONER_DATABASE_URL`. Created here
-- with the same `pg_roles` guard as 0083/0323 (the role is cluster-wide and
-- may already exist). Its security attributes (no LOGIN, superuser,
-- createdb, createrole, replication or bypassrls) and that neither app role
-- can assume it are asserted when this migration runs, once per database.
-- `scripts/check_provisioning_privileges.sh` re-checks the same on every
-- deploy, but against a fresh CI database built from this tree: it catches a
-- later migration that changes them, never a change made directly on prod.
-- The live prod role is checked by the worker's boot self-check
-- (`provisioning::provisioner_self_check`), which refuses to start if the
-- group role has any of those attributes. Concurrent scratch databases
-- (the test suites) race the CREATE on the shared catalog, so that one
-- statement is retried on the catalog-race SQLSTATEs (and on a lock wait
-- that outlasts this migration's `lock_timeout`).
DO $$
DECLARE
    attempt int := 0;
BEGIN
    LOOP
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'coralswarm_provisioner') THEN
                CREATE ROLE coralswarm_provisioner
                    NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
            END IF;
            EXIT;
        EXCEPTION WHEN unique_violation OR duplicate_object OR internal_error OR lock_not_available THEN
            attempt := attempt + 1;
            IF attempt >= 10 THEN
                RAISE;
            END IF;
            PERFORM pg_sleep(0.05 * attempt);
        END;
    END LOOP;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'coralswarm_provisioner'
                AND (rolcanlogin OR rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls)) THEN
        RAISE EXCEPTION 'coralswarm_provisioner has unexpected role attributes';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles r
                WHERE r.rolname IN ('coralswarm_app', 'coralswarm_tenant_app')
                  AND pg_catalog.pg_has_role(r.oid, 'coralswarm_provisioner', 'MEMBER')) THEN
        RAISE EXCEPTION 'coralswarm_provisioner must not be assumable by an app role';
    END IF;
END $$;

REVOKE ALL ON FUNCTION tenant_provisioning_store_credential(text, bytea) FROM PUBLIC;
REVOKE ALL ON FUNCTION tenant_provisioning_delete_credential(text) FROM PUBLIC;
REVOKE ALL ON FUNCTION tenant_provisioning_insert_routing(text, uuid, text, text, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION tenant_provisioning_record_schema_version(text, text, interval, bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION tenant_provisioning_activate(text, bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION tenant_provisioning_delete_routing(text) FROM PUBLIC;
REVOKE ALL ON FUNCTION tenant_provisioning_enqueue(text, uuid, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION tenant_provisioning_claim(text, interval, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION tenant_provisioning_set_step(text, text, interval, text, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION tenant_provisioning_defer(text, text, integer, text, interval, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION tenant_provisioning_record_create_intent(text, text, interval) FROM PUBLIC;
REVOKE ALL ON FUNCTION tenant_provisioning_clear_create_intent(text, text, interval) FROM PUBLIC;
REVOKE ALL ON FUNCTION tenant_provisioning_record_database(text, text, interval, text, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION tenant_provisioning_record_credential_stored(text, text, interval) FROM PUBLIC;
REVOKE ALL ON FUNCTION tenant_provisioning_fail(text, text, text, boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION tenant_provisioning_cancel(text) FROM PUBLIC;
REVOKE ALL ON FUNCTION tenant_provisioning_mark_clerk_org_cleanup_done(text) FROM PUBLIC;
REVOKE ALL ON FUNCTION tenant_provisioning_retry(text) FROM PUBLIC;

-- The worker: every definer that writes `tenant_databases` or the
-- credential table, or moves a job past `verify_org`. SELECT on the tables
-- it reads back (the job row, its routing row through `migrate_all`, the
-- credential `credentials::store` compares against). No table write.
GRANT USAGE ON SCHEMA public TO coralswarm_provisioner;
REVOKE ALL ON tenant_databases, tenant_provisioning_jobs, tenant_db_credentials FROM coralswarm_provisioner;
GRANT SELECT ON tenant_databases, tenant_provisioning_jobs, tenant_db_credentials TO coralswarm_provisioner;
GRANT EXECUTE ON FUNCTION tenant_provisioning_store_credential(text, bytea) TO coralswarm_provisioner;
GRANT EXECUTE ON FUNCTION tenant_provisioning_delete_credential(text) TO coralswarm_provisioner;
GRANT EXECUTE ON FUNCTION tenant_provisioning_insert_routing(text, uuid, text, text, text) TO coralswarm_provisioner;
GRANT EXECUTE ON FUNCTION tenant_provisioning_record_schema_version(text, text, interval, bigint) TO coralswarm_provisioner;
GRANT EXECUTE ON FUNCTION tenant_provisioning_activate(text, bigint) TO coralswarm_provisioner;
GRANT EXECUTE ON FUNCTION tenant_provisioning_delete_routing(text) TO coralswarm_provisioner;
GRANT EXECUTE ON FUNCTION tenant_provisioning_claim(text, interval, text) TO coralswarm_provisioner;
GRANT EXECUTE ON FUNCTION tenant_provisioning_set_step(text, text, interval, text, text) TO coralswarm_provisioner;
GRANT EXECUTE ON FUNCTION tenant_provisioning_defer(text, text, integer, text, interval, text) TO coralswarm_provisioner;
GRANT EXECUTE ON FUNCTION tenant_provisioning_record_create_intent(text, text, interval) TO coralswarm_provisioner;
GRANT EXECUTE ON FUNCTION tenant_provisioning_clear_create_intent(text, text, interval) TO coralswarm_provisioner;
GRANT EXECUTE ON FUNCTION tenant_provisioning_record_database(text, text, interval, text, text) TO coralswarm_provisioner;
GRANT EXECUTE ON FUNCTION tenant_provisioning_record_credential_stored(text, text, interval) TO coralswarm_provisioner;
GRANT EXECUTE ON FUNCTION tenant_provisioning_fail(text, text, text, boolean) TO coralswarm_provisioner;

-- The app role: reads both tables (it holds the KEK and serves the resolver
-- chain; A2 reads job status) and writes neither directly. It may ask for an
-- org to be provisioned (enqueue), cancelled, retried after a failure, and
-- record that A2 dealt with an orphaned Clerk org; nothing that advances a
-- job or writes routing. Same role-existence guard as 0321: dev/CI clusters
-- may predate 0083's role. `scripts/check_provisioning_privileges.sh`
-- re-checks all of this before every deploy, on a fresh database built from
-- the tree (not on prod).
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'coralswarm_app') THEN
        REVOKE ALL ON tenant_db_credentials FROM coralswarm_app;
        GRANT SELECT ON tenant_db_credentials TO coralswarm_app;
        REVOKE ALL ON tenant_provisioning_jobs FROM coralswarm_app;
        GRANT SELECT ON tenant_provisioning_jobs TO coralswarm_app;
        GRANT EXECUTE ON FUNCTION tenant_provisioning_enqueue(text, uuid, text) TO coralswarm_app;
        GRANT EXECUTE ON FUNCTION tenant_provisioning_cancel(text) TO coralswarm_app;
        GRANT EXECUTE ON FUNCTION tenant_provisioning_mark_clerk_org_cleanup_done(text) TO coralswarm_app;
        GRANT EXECUTE ON FUNCTION tenant_provisioning_retry(text) TO coralswarm_app;
        -- The hardening this migration preserves, re-asserted so a later
        -- GRANT cannot silently undo it without failing here.
        IF has_table_privilege('coralswarm_app', 'public.tenant_databases', 'INSERT,UPDATE,DELETE') THEN
            RAISE EXCEPTION 'coralswarm_app must not hold table write on tenant_databases';
        END IF;
        IF has_table_privilege('coralswarm_app', 'public.tenant_provisioning_jobs', 'INSERT,UPDATE,DELETE') THEN
            RAISE EXCEPTION 'coralswarm_app must not hold table write on tenant_provisioning_jobs';
        END IF;
        IF has_function_privilege('coralswarm_app', 'public.tenant_provisioning_insert_routing(text, uuid, text, text, text)', 'EXECUTE')
           OR has_function_privilege('coralswarm_app', 'public.tenant_provisioning_activate(text, bigint)', 'EXECUTE') THEN
            RAISE EXCEPTION 'coralswarm_app must not execute the routing definers';
        END IF;
    END IF;
END $$;
