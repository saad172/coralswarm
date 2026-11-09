-- Migration: 0090_member_ocean_columns
-- Description: member-private-oceans PR 1
--              (docs/plans/member-private-oceans.md §2.1/§7) — SCHEMA ONLY,
--              deliberately INERT. Adds the columns/constraints/indexes a
--              future `kind='member'` ocean needs. No Rust in this repo reads
--              or writes any of this yet: the `Graph` struct, the
--              reachability predicate, and every behavioral change land in PR
--              2 and later, gated behind `MEMBER_OCEANS_ENABLED`. Landing the
--              schema alone, ahead of any code path that touches it, means
--              this migration can run against the one live tenant with ZERO
--              observable change to the running system.
--
--              `graphs` is a SHARED table (present in both migration trees;
--              `check_schema_parity.sh` diffs a schema-only `pg_dump` of
--              both), so this DDL is byte-identical to its paired copy
--              `migrations_tenant/0005_member_ocean_columns.sql` — only the
--              header prose differs, per this repo's existing pairing
--              convention (e.g. 0085_jobs_account_id / 0004_jobs_account_id;
--              0084_bootstrap_scope_functions / 0003_bootstrap_scope_functions).
--
--              ── owner_user_id UUID, deliberately NO foreign key ──
--              `users` is a control-tree table (accounts/users/sessions/etc.)
--              that does not exist in an org's tenant database at all — an FK
--              here could not even be DECLARED on the tenant tree, and
--              `check_schema_parity.sh` would flag the asymmetry if one tree
--              somehow got one anyway. This matches the existing bare
--              `clerk_org_id TEXT`-with-no-FK convention this repo already
--              uses for a tenant-tree row referencing a control-tree id
--              (`org_revocations`/`org_membership_tombstones`, migrations
--              0081/0082, carry the identical reasoning for the same
--              structural constraint). NULL on every row that exists today —
--              no code creates a `kind='member'` row yet, and this migration
--              adds no backfill.
--
--              ── archived_at TIMESTAMPTZ ──
--              NULL on every row until PR 7's lifecycle work stamps it ONCE
--              at offboard time (plan §2.6: never reset by a webhook replay,
--              so retention is always >= 60 days, never less). Every new
--              unique index below carries `archived_at IS NULL` so an
--              archived ocean drops out of cardinality checks instead of
--              permanently blocking a fresh replacement ocean for the same
--              owner.
--
--              ── graphs_kind_check widens by exactly ONE value: 'member' ──
--              Deliberately NOT 'channel' (Stage 2, plan §4). An unreachable
--              kind that nothing creates yet is dead surface, and every
--              Stage-1 exclusion the reachability predicate will need to
--              write (PR 2) would have to be written against a value that
--              cannot yet exist. Stage 2 widens this CHECK again, in its own
--              migration, alongside
--              `CHECK (kind <> 'channel' OR (owner_user_id IS NULL AND NOT is_primary))`.
--
--              ── graphs_member_requires_owner ──
--              `(kind = 'member') = (owner_user_id IS NOT NULL)` — a single
--              boolean-equality invariant rather than two one-directional
--              CHECKs, so it reads as the one rule it is: a graph is
--              owner-scoped if and only if its kind is 'member'.
--              Trivially satisfied by every row that exists today: no row can
--              have `kind='member'` yet (the OLD `graphs_kind_check`, being
--              replaced by this same migration, did not even permit that
--              value), and `owner_user_id` is NULL on every existing row
--              (brand-new column, no backfill) — so the left side is FALSE
--              for every row and the right side (`NULL IS NOT NULL`) is FALSE
--              for every row, and FALSE = FALSE holds unconditionally. This
--              migration adds the CHECK and immediately makes it hold; it
--              does not need to reconcile any pre-existing data.
--
--              ── is_primary cardinality: per-account -> per-owner ──
--              `uq_graphs_one_primary_per_account` (0026_ocean_model, bare
--              `WHERE is_primary`, unconditional on `account_id`) is DROPPED
--              and replaced by two indexes, because a future member ocean
--              needs its OWN primary designation independent of the
--              account-wide primary (MCP's `resolve_primary_ocean` needs an
--              `is_primary` row to exist for the CALLER's own scope, not just
--              once per account — plan §2.1):
--
--                * `uq_graphs_one_primary_account_wide` — the SAME rule as
--                  before (one `is_primary` row per `account_id`) PLUS
--                  `owner_user_id IS NULL AND archived_at IS NULL`. Every row
--                  that exists today has `owner_user_id` NULL (brand-new
--                  column) and `archived_at` NULL (brand-new column), so this
--                  widened predicate is satisfied by EXACTLY the same set of
--                  rows the old predicate was satisfied by — a no-op in
--                  effect for all data that exists before this migration
--                  runs. It only starts excluding rows once PR 5+ actually
--                  writes an owner-scoped or archived row, which is the
--                  intended behavior change and cannot happen before then.
--                * `uq_graphs_one_primary_per_owner` — the new, additive
--                  rule: at most one `is_primary` row per
--                  `(account_id, owner_user_id)` where `owner_user_id IS NOT
--                  NULL`. No row satisfies this predicate's `owner_user_id IS
--                  NOT NULL` clause today (see above), so this index starts
--                  empty and enforces nothing yet — it exists so the
--                  invariant is already in place the moment PR 5 starts
--                  inserting owner-scoped rows, rather than being a
--                  behind-schedule follow-up.
--
--              ── (resolved) one PRIMARY member ocean per owner, not one
--                 member ocean per owner ──
--              An earlier draft of the plan's §2.1 also created
--              `uq_graphs_one_member_ocean_per_owner`, capping an owner at
--              ONE non-archived `kind='member'` row outright. That was a
--              drafting error: it contradicts the plan's §5 decision
--              "Secondary member oceans | Allowed -- one is_primary per
--              owner, others optional", and it would have rejected a
--              member's second private ocean before the feature ever
--              shipped. Dropped rather than carried forward.
--
--              `uq_graphs_one_primary_per_owner` above already enforces the
--              invariant that actually matters -- one PRIMARY per owner --
--              and it is also the right ON CONFLICT target for
--              provisioning's race-safe insert (the auto-provisioned ocean
--              is `is_primary = true`, so two concurrent first-sight
--              provisions for the same member collide on it and one wins,
--              exactly as the removed index would have done). Secondary,
--              non-primary member oceans are unconstrained by design.
--
--              ── graphs_archived_at_idx ──
--              Partial index on `archived_at` (only rows where it is NOT
--              NULL — i.e. only archived rows, a small minority even once the
--              feature ships) for the future reconciler sweep / purge worker
--              (plan §2.6/PR 7-8) to find due-for-purge oceans without a
--              table scan. Empty and free today: no row has a non-NULL
--              `archived_at` yet.
--
--              ── Live-tenant safety ──
--              `graphs` holds one row per ocean — tiny (single digits per
--              tenant today: `testorg`'s tenant DB has exactly one row).
--              `MULTITENANT_ENABLED=1` is live with that one real tenant, so
--              this migration runs against production data. Mirroring
--              0083_consumer_rls's discipline: `SET LOCAL lock_timeout =
--              '3s'` up front, so a contended `ALTER TABLE`/`CREATE INDEX`
--              here aborts loudly and retriably instead of blocking
--              indefinitely behind a long-running transaction.
--              `CREATE INDEX CONCURRENTLY` is unnecessary here — it exists to
--              avoid a `SHARE` lock stalling writes on a large, hot table
--              (see 0059/0074/0075's headers), and it cannot run inside a
--              transaction block at all (sqlx wraps a migration file in one
--              by default; a `-- no-transaction` migration is reserved for
--              exactly that case). Given `graphs`' size, a plain `CREATE
--              INDEX` under the 3s lock_timeout above is simpler, correct,
--              and provably fast; reaching for `CONCURRENTLY`'s extra
--              ceremony (separate single-statement files, no-transaction
--              directive, INVALID-index cleanup risk) here would be
--              overhead with no corresponding benefit.
--
-- Depends on: 0026_ocean_model (graphs.is_primary,
--             uq_graphs_one_primary_per_account, uq_graphs_one_org_per_account),
--             0022_teams (graphs.kind CHECK), 0083_consumer_rls (graphs RLS —
--             unaffected: the account_isolation policy predicate does not
--             reference any column touched here).
--
-- Down:
--   DROP INDEX IF EXISTS graphs_archived_at_idx;
--   DROP INDEX IF EXISTS uq_graphs_one_primary_per_owner;
--   DROP INDEX IF EXISTS uq_graphs_one_primary_account_wide;
--   CREATE UNIQUE INDEX IF NOT EXISTS uq_graphs_one_primary_per_account
--       ON graphs (account_id) WHERE is_primary;
--   ALTER TABLE graphs DROP CONSTRAINT IF EXISTS graphs_member_requires_owner;
--   ALTER TABLE graphs DROP CONSTRAINT IF EXISTS graphs_kind_check;
--   ALTER TABLE graphs
--       ADD CONSTRAINT graphs_kind_check CHECK (kind IN ('personal', 'team', 'org'));
--   ALTER TABLE graphs DROP COLUMN IF EXISTS archived_at;
--   ALTER TABLE graphs DROP COLUMN IF EXISTS owner_user_id;

SET LOCAL lock_timeout = '3s';

-- ── Columns ──────────────────────────────────────────────────────────────────
ALTER TABLE graphs ADD COLUMN IF NOT EXISTS owner_user_id UUID;   -- bare uuid, no FK
ALTER TABLE graphs ADD COLUMN IF NOT EXISTS archived_at TIMESTAMPTZ;

-- ── kind CHECK: widen to include 'member' only ──────────────────────────────
ALTER TABLE graphs DROP CONSTRAINT IF EXISTS graphs_kind_check;
ALTER TABLE graphs ADD CONSTRAINT graphs_kind_check
  CHECK (kind IN ('personal','team','org','member'));

-- ── Owner-scoping invariant ──────────────────────────────────────────────────
ALTER TABLE graphs ADD CONSTRAINT graphs_member_requires_owner
  CHECK ((kind = 'member') = (owner_user_id IS NOT NULL));

-- ── Cardinality ──────────────────────────────────────────────────────────────
DROP INDEX IF EXISTS uq_graphs_one_primary_per_account;
CREATE UNIQUE INDEX uq_graphs_one_primary_account_wide
  ON graphs (account_id) WHERE is_primary AND owner_user_id IS NULL AND archived_at IS NULL;
CREATE UNIQUE INDEX uq_graphs_one_primary_per_owner
  ON graphs (account_id, owner_user_id) WHERE is_primary AND owner_user_id IS NOT NULL AND archived_at IS NULL;
CREATE INDEX graphs_archived_at_idx ON graphs (archived_at) WHERE archived_at IS NOT NULL;
