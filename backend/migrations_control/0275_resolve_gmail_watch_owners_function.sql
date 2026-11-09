-- Migration 0275: resolve_gmail_watch_owners(text) — every account watching
--   one mailbox (control-only, no tenant twin — same reasoning as 0187: the
--   cross-account discovery problem only exists on the control plane).
--
--   Supersedes 0187's `resolve_gmail_watch_owner(text)` for push dispatch.
--   That function fails closed — ZERO rows — the moment the same Gmail
--   address is watched under more than one CoralSwarm account, on the
--   theory that routing one mailbox's history diff into "the wrong account"
--   was a privacy bug. It is not a wrong-account problem at all: a mailbox
--   connected under both a personal account and an org account (the normal
--   shape for anyone who uses CoralSwarm in both) holds its OWN OAuth token,
--   its OWN `google_watch_channels` cursor and its OWN atoms under EACH
--   account, and the push notification is a bare "this mailbox changed"
--   trigger for every one of them. Returning nothing starves BOTH accounts:
--   neither live follow nor deletion propagation ever runs for that mailbox
--   again, silently (the caller logs "unknown mailbox" and drops).
--
--   `routes::google_push::run_gmail_sync_for_email` now fans out: one pass
--   per returned row, each against that account's own cursor, token and
--   content plane. Same guards as 0187 (`p_email <> ''`, `resource_id <> ''`,
--   `owner_user_id IS NOT NULL`), one row per account (a mailbox has one
--   Gmail watch row per account by `UNIQUE (account_id, surface,
--   resource_id)`), ordered for deterministic dispatch.
--
--   0187's function is left in place (merged migrations are immutable and
--   nothing else depends on its removal); it simply has no caller any more.
--
-- Depends on: 0186_google_gmail_watch_owner_and_threads.sql (the columns this reads).
-- Down:
--   REVOKE EXECUTE ON FUNCTION resolve_gmail_watch_owners(text) FROM coralswarm_app, CURRENT_USER;
--   REVOKE EXECUTE ON FUNCTION resolve_gmail_watch_owners(text) FROM PUBLIC;
--   DROP FUNCTION IF EXISTS resolve_gmail_watch_owners(text);

CREATE OR REPLACE FUNCTION resolve_gmail_watch_owners(p_email text)
RETURNS TABLE(account_id uuid, owner_user_id uuid)
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT gwc.account_id, gwc.owner_user_id
    FROM google_watch_channels gwc
    WHERE gwc.surface = 'gmail'
      AND gwc.resource_id = p_email
      AND p_email <> ''
      AND gwc.resource_id <> ''
      AND gwc.owner_user_id IS NOT NULL
    ORDER BY gwc.account_id
$$;

REVOKE EXECUTE ON FUNCTION resolve_gmail_watch_owners(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION resolve_gmail_watch_owners(text) TO coralswarm_app, CURRENT_USER;
