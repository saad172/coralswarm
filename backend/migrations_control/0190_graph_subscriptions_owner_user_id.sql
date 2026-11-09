-- Migration: 0190_graph_subscriptions_owner_user_id
-- Description: m365 enterprise train PR 3.3 ("org thread opt-in + live
--   follow") — add the owner dimension to `graph_subscriptions`, same shape
--   as `oauth_tokens`' own `owner_user_id` (migration `0175`).
--
--   `graph_subscriptions` is keyed `UNIQUE (account_id, resource)` today —
--   fine while every Microsoft connection is account-wide (one mailbox per
--   account), but wrong the moment an ORG account can hold more than one
--   member's Outlook connection (Train 3.3 lifts the gate that made this
--   unreachable — see `docs/connectors/outlook.md` §6). Two org members each
--   opting a thread in both call `ensure_mail_subscriptions` for the SAME
--   `(account_id, "me/mailFolders('inbox')/messages")` pair, each with their
--   OWN delegated token (Graph resolves `/me` to whichever token created the
--   subscription — a mailbox-scoped subscription, not an account-scoped one).
--   With no owner dimension, `ensure_subscription_for`'s "does a live
--   subscription already exist for `(account_id, resource)`" check sees the
--   FIRST member's subscription as satisfying the SECOND member's opt-in and
--   skips creating one — the second member's mailbox is silently never
--   watched at all, not merely mis-renewed.
--
--   This also closes a documented, named gap in `connectors::
--   microsoft_disconnect::purge_microsoft_connection`'s own doc comment
--   (Train 3.7, `#685`): "`graph_subscriptions` has no per-member column to
--   scope by... a `Member` purge... leaves mail/transcript subscriptions
--   alone. Revisit when [per-member Outlook] gives Microsoft real per-member
--   mailboxes." This is that revisit.
--
--   `owner_user_id uuid` is nullable, added with no backfill: `NULL` means
--   "account-wide" (every existing row — Personal-plane mail/Teams/transcript
--   subscriptions, and every already-shipped Teams-resource subscription,
--   which stays account-wide-keyed forever — see below), a non-NULL value
--   means "this member's own mailbox subscription". Same two disjoint PARTIAL
--   unique indexes as `oauth_tokens` (`0175`), for the identical reason (a
--   bare 3-column `(account_id, resource)` constraint can't express both
--   shapes; Postgres unique indexes treat NULL as distinct, so a plain 3rd
--   column would let ON CONFLICT silently duplicate rows instead of
--   upserting them):
--
--     * `uq_graph_subscriptions_account_wide` — the pre-existing 2-column
--       key, scoped to `owner_user_id IS NULL`. At most one account-wide
--       subscription per resource — covers Personal-plane mail, and EVERY
--       Teams/transcript resource on both planes (Teams channel/chat/
--       transcript resource STRINGS already disambiguate by container, so
--       they never needed an owner dimension and keep using this index
--       unconditionally — see `connectors::microsoft::ensure_subscription_for`'s
--       call sites).
--     * `uq_graph_subscriptions_per_owner` — the same 2 columns PLUS
--       `owner_user_id`, scoped to `owner_user_id IS NOT NULL`. At most one
--       subscription per member per resource — lets two org members each
--       hold their own live `inbox`/`sentitems` mail subscription under the
--       same shared org `account_id`.
--
--   Application code picks exactly one shape explicitly
--   (`crate::connectors::TokenOwner::AccountWide | Member`) on every
--   read/write, same discipline `0175`'s header already states for
--   `oauth_tokens` — there is deliberately no
--   `owner_user_id IS NULL OR owner_user_id = $n` fallback anywhere.
--
--   `graph_subscriptions` is a SHARED tenant table (present on both the
--   control and tenant trees, per `scripts/check_schema_parity.sh`'s
--   `CONTROL_ONLY_RLS_TABLES` roster — it carries the consumer-plane
--   `account_isolation` RLS policy there, and NO tenant-plane RLS at all, per
--   `migrations_tenant/0009_tenant_rls.sql`'s own "deliberately NOT covered"
--   list — it has no ocean/`graph_id` anchor to hang a `member_ocean_
--   isolation` policy off of). Its TABLE SHAPE, unlike its RLS, must still
--   match on both trees, so this is paired byte-for-byte into
--   `migrations_tenant/0068_graph_subscriptions_owner_user_id.sql`.
--
-- Down:
--   DROP INDEX IF EXISTS uq_graph_subscriptions_per_owner;
--   DROP INDEX IF EXISTS uq_graph_subscriptions_account_wide;
--   ALTER TABLE graph_subscriptions
--     ADD CONSTRAINT graph_subscriptions_account_id_resource_key
--       UNIQUE (account_id, resource);
--   ALTER TABLE graph_subscriptions DROP COLUMN IF EXISTS owner_user_id;

--
--   MIXED-VERSION CUTOVER NOTE (CodeRabbit round 1): old binaries write
--   `ON CONFLICT (account_id, resource)`, which stops matching once the
--   named UNIQUE constraint below is dropped (neither partial index is a
--   valid arbiter for that spec). Postgres then ERRORS those writes rather
--   than corrupting anything — fail-closed, but noisy. This codebase's
--   release process already makes the window trivial: migrations are
--   applied manually per-org AT deploy time by the single deployer (see
--   plan release checklist), the only writers are this app's own workers,
--   and there is no rolling fleet of old binaries. Apply this migration
--   together with the deploy of the binary that contains PR 3.3 —
--   documented in the plan's release checklist rather than engineered
--   around with a multi-phase constraint dance this deployment shape
--   doesn't need.

ALTER TABLE graph_subscriptions ADD COLUMN IF NOT EXISTS owner_user_id UUID;

ALTER TABLE graph_subscriptions
    DROP CONSTRAINT IF EXISTS graph_subscriptions_account_id_resource_key;

CREATE UNIQUE INDEX IF NOT EXISTS uq_graph_subscriptions_account_wide
    ON graph_subscriptions (account_id, resource)
    WHERE owner_user_id IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_graph_subscriptions_per_owner
    ON graph_subscriptions (account_id, resource, owner_user_id)
    WHERE owner_user_id IS NOT NULL;
