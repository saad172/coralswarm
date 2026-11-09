-- Migration: 0189_microsoft_calendar_sync_state
-- Description: m365 enterprise train PR 3.8 ("Calendar default-on for org
--   members"). Per-member sync bookkeeping for Microsoft Calendar
--   (`microsoft_calendar` source type, `connectors::microsoft::fetch_events`)
--   on the org plane: `workers::microsoft_calendar_sync`'s tenant-loop tick
--   sweeps this table to decide, per (account, member), whether to pull that
--   member's calendar, and to record the per-member off switch (decision #1).
--
--   ── Renumbered tenant half ──
--   The plan doc (`.scratch/m365-enterprise/plan.md`) reserved control 0189 /
--   tenant 0066 for this PR before it started. By the time this PR actually
--   ran, tenant 0066 had already been taken (gsuite Train G2 PR 3's
--   `gmail_watched_threads`, `migrations_tenant/
--   0066_google_gmail_watch_owner_and_threads.sql`, merged as part of #686) —
--   same "merged wins, renumber" precedent the ledger already applied twice
--   this week (#683 vs #686, #686 vs #688). Control 0189 was still free
--   (0188 is PR 3.2's `microsoft_admin_consents`), so only the tenant twin
--   renumbers: this migration's tenant twin is **0067**, not 0066 — see
--   `migrations_tenant/0067_microsoft_calendar_sync_state.sql`.
--
--   ── Why default-on needs no seeding, no backfill ──
--   Decision #1: calendar sync is enabled the moment a member's delegated
--   Microsoft token exists — not a separate opt-in gate. Rather than write a
--   row at connect time (which would couple this migration to
--   `routes::oauth::callback`'s still-dormant org-member Microsoft write
--   path — m365 PR 3.2 §11's "known gap, not closed by this PR"), the
--   tenant-loop tick lazily upserts a row (`sync_enabled = TRUE` default) the
--   first time it discovers a member's Microsoft token with no row yet — see
--   `workers::microsoft_calendar_sync`'s module doc. A member who is never
--   swept (no Microsoft Member token yet) simply has no row, which reads
--   identically to "sync enabled, never yet run" the moment one IS
--   discovered — there is nothing to backfill.
--
--   ── Structural precedent ──
--   Google Calendar's `google_calendar_sync_state` (0182/0063) is push-based
--   (columns on the shared `google_watch_channels` watch-channel row) — no
--   direct analog here, because Microsoft Calendar has no push channel at
--   all (pull only, `docs/connectors/outlook.md` C7). This table is closer
--   in shape to `gmail_watched_threads` (0186/0066): one row per (account,
--   member), a `graph_id` anchor for tenant-plane RLS, an
--   `account_isolation`/`member_ocean_isolation` twin split. It differs from
--   both precedents in cardinality: `gmail_watched_threads` is one row PER
--   THREAD (many per member); this is one row PER MEMBER (the whole
--   calendar — Graph's `/me/events` has no natural "thread" unit to key on).
--
--   ── Column choices ──
--   `owner_user_id` — the member this row is for. `NOT NULL`: unlike
--   `google_watch_channels.owner_user_id`, this table exists ONLY for the
--   per-member case — there is no "account-wide calendar sync state" row to
--   leave the column nullable for. `microsoft.rs::trigger_sync`'s existing
--   user-triggered HTTP path (personal plane, `TokenOwner::AccountWide`) is
--   untouched by this PR and needs no state row at all.
--
--   `graph_id` — the member's own primary private ocean
--   (`routes::slack::resolve_primary_graph`'s org-member arm). `NOT NULL`,
--   deliberately: a row is written only AFTER the tick successfully resolves
--   the member's ocean (which `resolve_primary_graph` does NOT auto-create on
--   the org plane — provisioning is `workers::member_ocean_reconciler`'s job),
--   never before. A member whose ocean is not yet provisioned is simply not
--   written yet, and re-tried the next tick — see the worker's own module
--   doc. This also keeps the tenant twin's `member_ocean_isolation` RLS
--   policy sound: `graph_id IN (subquery)` is NULL (never TRUE) for a NULL
--   `graph_id`, which would make a nullable-graph_id row permanently
--   invisible even under `begin_tenant_system_scoped` — a real correctness
--   trap this migration avoids by construction rather than by convention.
--
--   `sync_enabled` — the per-member off switch (decision #1). `BOOLEAN NOT
--   NULL DEFAULT TRUE`: default-on is the column default itself, not app
--   logic layered on top of a nullable column — a row existing at all means
--   "this member has been seen with a Microsoft token and an ocean"; whether
--   it still says TRUE is the entire off switch.
--
--   `last_synced_at` / `last_error` / `last_error_at` — same observability
--   shape as `google_watch_channels`' renewal bookkeeping (this migration's
--   structural cousin for "record of a periodic background pass", even
--   though the push/pull shapes differ).
--
--   ── Sync cursor: deliberately NOT in this migration ──
--   `docs/connectors/outlook.md` C6/C7 already names Calendar's missing
--   `@odata.nextLink`/delta cursor as a known gap. This PR's brief allows
--   closing it if Graph's `calendarView/delta` maps cleanly onto this table —
--   it does not, cheaply, inside this PR's scope (delta is a different
--   endpoint shape from the `$top=20` list `fetch_events` calls today, and
--   wiring it is a `connectors::microsoft` change, not a schema one) — so no
--   cursor column is added here. See `workers::microsoft_calendar_sync`'s
--   module doc and `docs/connectors/outlook.md` for the deferral, recorded
--   loudly per this PR's brief.
--
--   ── No RLS gap: `account_isolation` here, `member_ocean_isolation` on the
--   tenant twin ── same split `gmail_watched_threads` (0186/0066) uses.
--
--   ── `WITH CHECK` (CodeRabbit round 1 on PR #692) ──
--   `gmail_watched_threads` (0186/0066), this migration's own named
--   structural precedent above, turned out to carry only a `USING` clause on
--   BOTH planes — not a pattern to copy for a table whose entire point is a
--   durable `(account_id, owner_user_id, graph_id)` triple written by an
--   unattended tenant-loop tick with no human review of any individual row.
--   Postgres applies `USING` to writes too when `WITH CHECK` is absent, which
--   left this table able to accept an INSERT/UPDATE pairing this account's
--   `graph_id` with an `owner_user_id` that is NOT that graph's real owner
--   (e.g. a bug or a compromised session writing account A's row against
--   account B's `graph_id`, or against a `graph_id` that is A's own but a
--   DIFFERENT member's private ocean). `seat_grant_sources` (0184) is the
--   right precedent instead: `account_isolation` split into `USING` (read)
--   plus a `WITH CHECK` that additionally re-derives ownership of the
--   referenced row from a live subquery against the table that actually
--   owns that invariant — there `teams`, here `graphs`.
--
--   The subquery requires the referenced `graphs` row to be (a) in the SAME
--   account as this row's own `account_id` — re-asserted explicitly rather
--   than assumed, exactly like `seat_grant_sources`' `team_id IN (SELECT id
--   FROM teams WHERE account_id = current_setting(...))` — and (b) genuinely
--   THIS row's `owner_user_id`'s primary member ocean: `kind = 'member'`
--   (the `graphs_member_requires_owner` CHECK from 0090 already ties
--   `kind='member'` to a non-NULL `owner_user_id` 1:1, so this is the exact
--   predicate `resolve_org_member_primary_graph`'s own read query uses,
--   `routes::slack.rs`) and `archived_at IS NULL` (an archived ocean is not
--   a live destination for a fresh sync-state row). Because this subquery
--   runs as the SAME low-privilege role the INSERT/UPDATE itself runs as, it
--   is automatically filtered by `graphs`' own `account_isolation` policy
--   too — a belt-and-suspenders effect, not a substitute for the explicit
--   `account_id` equality restated below (which stays correct even for a
--   BYPASSRLS/owner-role writer that graphs' own policy would not filter).
--
-- Depends on: 0175_oauth_tokens_owner_user_id (the Member token dimension
--             this table's discovery reads), 0188_microsoft_admin_consents
--             (freshest control-only precedent for this migration's number).
-- Down:
--   DROP POLICY IF EXISTS account_isolation ON microsoft_calendar_sync_state;
--   DROP TABLE IF EXISTS microsoft_calendar_sync_state;

CREATE TABLE IF NOT EXISTS microsoft_calendar_sync_state (
    id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id       UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    owner_user_id    UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    graph_id         UUID        NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
    sync_enabled     BOOLEAN     NOT NULL DEFAULT TRUE,
    last_synced_at   TIMESTAMPTZ,
    last_error       TEXT,
    last_error_at    TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (account_id, owner_user_id)
);

ALTER TABLE microsoft_calendar_sync_state ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON microsoft_calendar_sync_state;
CREATE POLICY account_isolation ON microsoft_calendar_sync_state
    USING (account_id = current_setting('app.account_id')::uuid)
    WITH CHECK (
        account_id = current_setting('app.account_id')::uuid
        AND EXISTS (
            SELECT 1 FROM graphs
            WHERE graphs.id = microsoft_calendar_sync_state.graph_id
              AND graphs.account_id = current_setting('app.account_id')::uuid
              AND graphs.owner_user_id = microsoft_calendar_sync_state.owner_user_id
              AND graphs.kind = 'member'
              AND graphs.archived_at IS NULL
        )
    );
