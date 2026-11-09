-- Settings launch 1.5 (decision 16): "Local only" (`account_prefs.prefs->>
-- 'egress_policy'`) is no longer an option.
--
-- `egress_policy` is a key inside the `account_prefs.prefs` JSONB document.
-- `account_prefs` is a CONTROL-plane table only (scripts/check_schema_parity.sh
-- CONTROL_TABLES), so there is no tenant-tree counterpart to this migration.
--
-- 1. Guard UPDATE: move any account still set to a cloud-forbidding value to
--    "Allow cloud". The founder confirmed no prod account uses Local only, so
--    this is expected to touch zero rows and no one is emailed; it stays as a
--    guard so the CHECK below cannot fail on stale data. "Forbidding" is what
--    the removed enforcer read: trim, lowercase, `_`/`-` as space, then
--    "local only" or "local".
-- 2. CHECK so the value cannot come back by any write path (the API validator
--    also rejects it). Added after the UPDATE in the same transaction, so it
--    validates against clean data. The prod tenant role is the table owner, so
--    no grants are needed.
--
-- Rolling deploy note: a pre-deploy backend that still accepts "Local only"
-- gets a database error (surfaced as 503) on that one write until the new
-- backend is live.
--
-- Down:
--   ALTER TABLE account_prefs DROP CONSTRAINT IF EXISTS account_prefs_egress_policy_not_local_only;
SET LOCAL lock_timeout = '3s';

UPDATE account_prefs
   SET prefs = jsonb_set(prefs, '{egress_policy}', '"Allow cloud"'::jsonb),
       updated_at = now()
 WHERE jsonb_typeof(prefs) = 'object'
   AND lower(regexp_replace(btrim(prefs->>'egress_policy', E' \t\r\n'), '[_-]', ' ', 'g'))
       IN ('local only', 'local');

ALTER TABLE account_prefs
    DROP CONSTRAINT IF EXISTS account_prefs_egress_policy_not_local_only;
ALTER TABLE account_prefs
    ADD CONSTRAINT account_prefs_egress_policy_not_local_only
    CHECK (
        jsonb_typeof(prefs) <> 'object'
        OR lower(regexp_replace(btrim(COALESCE(prefs->>'egress_policy', ''), E' \t\r\n'), '[_-]', ' ', 'g'))
           NOT IN ('local only', 'local')
    );
