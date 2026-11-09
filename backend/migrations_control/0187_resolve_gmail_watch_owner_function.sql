-- Migration 0187: resolve_gmail_watch_owner(text) bootstrap function
--   (gsuite Train G2 PR 3, control-only, no tenant twin — same reasoning as
--   0181/0183: an org tenant database holds exactly one account (ADR-0007),
--   so there is no cross-account discovery problem there).
--
--   RENUMBERED from the originally-announced 0186 after #683 merged holding
--   control 0185 (bumping this migration's sibling,
--   `0186_google_gmail_watch_owner_and_threads.sql`, off 0185 and onto 0186)
--   — merged wins; see that migration's own header. Purely a rename — no
--   content change.
--
--   `routes::google_push` (the Pub/Sub push webhook) authenticates an
--   inbound Gmail `users.watch` notification via its OIDC bearer token (see
--   that route's own module doc), but the decoded `message.data` payload
--   carries only `{emailAddress, historyId}` — no `account_id`, no
--   `owner_user_id`. Both have to be DISCOVERED from the email address
--   before any `begin_tenant_system_scoped` transaction can be opened —
--   exactly the same bootstrap problem `0183`'s `resolve_google_watch_
--   channel_account_id` solves for the Calendar webhook's `channel_id`, and
--   `0181`'s `resolve_google_watch_channels_due` solves for the renewal
--   sweep. This is that same pattern, keyed by `(surface, resource_id)`
--   instead: bare `(account_id, owner_user_id)` only — never any secret,
--   since Pub/Sub push has no per-notification shared secret of its own to
--   read back in the first place (the OIDC bearer token IS the whole trust
--   boundary, verified before this function is ever called).
--
--   `p_email <> ''` AND `resource_id <> ''` (0183's empty-placeholder
--   finding applies identically here): a `google_watch_channels` row created
--   by a sync-only write, before `connectors::google_gmail::ensure_watch`
--   has ever succeeded, defaults `resource_id` to whatever was bound at
--   insert time — for Gmail that is always the member's real email (never
--   blank, since ensure_watch is the only writer of gmail rows and it always
--   has a real email in hand) — but the guard costs nothing and keeps this
--   function byte-consistent with its two siblings rather than a special
--   case future maintainers have to remember is "safe here, unlike there".
--   `owner_user_id IS NOT NULL` is Gmail's OWN structural guarantee (the
--   AccountWide-never-for-Gmail invariant, design doc PR G2.3 item 3): a
--   Gmail row with no owner cannot be resolved to a member's private ocean,
--   so treat it exactly like "unknown" rather than resolving to a caller
--   with no destination.
--
--   ── Fail closed on a cross-account email collision (CodeRabbit round 1
--   finding #1 on PR #686, Major/security) ──
--   `google_watch_channels`' own uniqueness (`UNIQUE (account_id, surface,
--   resource_id)`, migration 0180) is scoped PER ACCOUNT — nothing stops the
--   SAME Gmail address from being connected under TWO different CoralSwarm
--   accounts (e.g. one person's Gmail OAuth'd under both a personal account
--   and an org account, or a stale/duplicate connection never cleaned up).
--   The original version of this function used a bare `LIMIT 1` with no
--   `ORDER BY` — on a genuine collision, that resolves to WHICHEVER matching
--   row Postgres happens to return first, which is not a cross-tenant data
--   LEAK (the push-triggered pass only ever acts within whichever account
--   it resolves to) but IS a correctness/privacy bug: an inbound Gmail
--   notification for account A's mailbox could route its history diff, and
--   therefore its content, into account B's ocean instead. The rewrite
--   below counts distinct candidate ACCOUNTS first and returns ZERO rows
--   (not an arbitrary pick) the moment there is more than one — the Rust
--   caller (`connectors::google_gmail::resolve_watch_owner_for_push`)
--   already treats "no rows" as "unknown mailbox, log and drop" (the
--   `routes::google_push` module doc's fail-closed-on-ambiguity contract),
--   so no caller-side change was needed, only this function.
--
--   Deliberately NOT a hard uniqueness constraint on `resource_id` alone
--   (an alternative CodeRabbit raised, e.g. a partial unique index mirroring
--   0182's `idx_google_watch_channels_channel_id_unique`): a real person
--   legitimately reconnecting the same Gmail address under a NEW CoralSwarm
--   account after disconnecting the old one is a supported flow this
--   function must not need a companion migration to unblock, and a hard
--   constraint would turn a transient overlap during that flow into an
--   INSERT failure rather than this function's own graceful "ambiguous, no
--   rows" resolution. Failing closed at the READ site (this function, the
--   only place ambiguity is actually consumed) is the narrower, safer fix.
--
-- Depends on: 0186_google_gmail_watch_owner_and_threads.sql (the columns this reads).
-- Down:
--   REVOKE EXECUTE ON FUNCTION resolve_gmail_watch_owner(text) FROM coralswarm_app, CURRENT_USER;
--   REVOKE EXECUTE ON FUNCTION resolve_gmail_watch_owner(text) FROM PUBLIC;
--   DROP FUNCTION IF EXISTS resolve_gmail_watch_owner(text);

CREATE OR REPLACE FUNCTION resolve_gmail_watch_owner(p_email text)
RETURNS TABLE(account_id uuid, owner_user_id uuid)
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    WITH candidates AS (
        SELECT gwc.account_id, gwc.owner_user_id
        FROM google_watch_channels gwc
        WHERE gwc.surface = 'gmail'
          AND gwc.resource_id = p_email
          AND p_email <> ''
          AND gwc.resource_id <> ''
          AND gwc.owner_user_id IS NOT NULL
    )
    SELECT c.account_id, c.owner_user_id
    FROM candidates c
    WHERE (SELECT count(DISTINCT c2.account_id) FROM candidates c2) = 1
    LIMIT 1
$$;

REVOKE EXECUTE ON FUNCTION resolve_gmail_watch_owner(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION resolve_gmail_watch_owner(text) TO coralswarm_app, CURRENT_USER;
