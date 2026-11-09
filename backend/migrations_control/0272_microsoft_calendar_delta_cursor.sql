-- Migration: 0272_microsoft_calendar_delta_cursor
-- Description: Microsoft Calendar deletions (fix/cal-deletions). The Graph
--   `calendarView/delta` cursor for one Microsoft Calendar connection, plus
--   the control-plane policy widening that lets the PERSONAL account-wide
--   connection keep its cursor on this same table.
--
--   ── The bug ──
--   `connectors::microsoft::fetch_events` was a cursorless
--   `GET /me/events?$top=20`: it only ever listed what currently exists, so
--   an Outlook event the user DELETED (or a meeting the organiser cancelled)
--   never came back and its coral stayed in the ocean forever — verified
--   live in prod 2026-10-07 (an event created, renamed, then deleted on an
--   org member's calendar; the renamed coral remained). Both callers —
--   `workers::microsoft_calendar_sync::sync_member` (org members, 30-minute
--   tick) and `routes::connectors::finish_ingest` (manual
--   `POST /v1/connectors/microsoft/sync`, personal plane + org member) —
--   had this gap. Google Calendar never did: its `syncToken` pass reports
--   `status=cancelled` and `connectors::google_calendar` retracts the coral.
--
--   ── The fix this migration supports ──
--   `connectors::microsoft_calendar` (new) syncs through Graph's
--   `GET /me/calendarView/delta?startDateTime=..&endDateTime=..`: the
--   first round is a full listing of a window (30 days back → 90 days
--   ahead), every later round GETs the stored `@odata.deltaLink` and
--   receives only what changed since — created/updated events as full
--   event objects and deleted ones as `{"id": .., "@removed": {..}}` —
--   exactly the shape Google's `syncToken` pass has. This is the Microsoft
--   twin of the `sync_token`/`backfill_page_token` pair on
--   `google_watch_channels` (0182); it lives HERE because this table is
--   already the one row per Microsoft Calendar connection that
--   `workers::microsoft_calendar_sync` reads and writes every tick — 0189's
--   own header deferred "the delta cursor" to exactly this spot.
--
--   ── Columns ──
--   `delta_link` — the `@odata.deltaLink` URL that completed the last
--   round (the whole URL, as Graph instructs: it encodes the window and
--   the state token). NULL = no round completed yet → next pass starts a
--   full listing. Cleared on a `410 Gone` / `SyncStateNotFound` reply (the
--   token expired upstream) so the following pass restarts from scratch.
--
--   `delta_resume_link` — the `@odata.nextLink` of a round that was cut
--   off by the per-pass page budget (`PAGES_PER_PASS`), so the next pass
--   continues the SAME round instead of restarting it. NULL when no round
--   is in progress. The twin of `google_watch_channels.backfill_page_token`.
--
--   `delta_window_end` — the `endDateTime` the current round's window was
--   anchored at. The window is frozen into the delta token the moment the
--   first request is made, so an event created beyond it would never be
--   reported; `connectors::microsoft_calendar::plan_round` re-anchors (a
--   fresh full round with a new window) once fewer than
--   `REANCHOR_MARGIN_DAYS` of lookahead remain. A full round also doubles
--   as a reconciliation of the stored corals (see `delta_reconciled_at`).
--
--   `delta_reconciled_at` — when the stored corals were last verified
--   against Graph one by one (`GET /me/events/{id}` → 404 / `isCancelled`
--   → retract). Runs at the end of every completed full round, and — rate-
--   limited by this timestamp — whenever a delta reply carries `@removed`
--   ids that match no stored coral (a deleted recurring SERIES surfaces
--   only as its removed occurrences, whose ids are not the stored series-
--   master id). Also what performs the one-time cleanup after deploy: the
--   first full round verifies every pre-existing `microsoft_calendar` coral
--   and retracts the ones whose event no longer exists.
--
--   ── Policy widening: the personal plane needs a row here too ──
--   0189's `WITH CHECK` admitted only `graphs.kind = 'member'` — the org
--   member shape, the only writer at the time. The manual
--   `POST /v1/connectors/microsoft/sync` on a PERSONAL account
--   (`TokenOwner::AccountWide`) now needs its own cursor row: keyed on the
--   account owner (`accounts.owner_user_id`, the one user a personal account
--   has) and the account's primary personal ocean. Same precedent as
--   `gmail_everything_mode` (0197): `(graphs.kind = 'personal' AND
--   graphs.is_primary = true)` is the personal-plane ownership anchor
--   (`resolve_primary_graph`'s Personal arm never sets `owner_user_id`).
--   The row's `owner_user_id` is additionally pinned to the account's real
--   owner through `accounts` (no RLS on that table — a plain lookup), so the
--   cross-owner rejection `tests/consumer_rls_pg.rs` §4g proves still holds
--   on the personal arm: a personal ocean paired with any OTHER user is
--   refused. The member arm is byte-identical to 0189's.
--
--   `sync_enabled`/`last_*` on a personal row are inert: the sync worker's
--   consumer entry is a structural no-op (`workers::microsoft_calendar_sync`
--   module doc), so nothing ever sweeps a personal row — the manual route
--   is the only reader/writer there.
--
--   The tenant twin (`migrations_tenant/0113_microsoft_calendar_delta_
--   cursor.sql`) adds the same four columns and leaves its
--   `member_ocean_isolation` policy untouched (org members only).
--
-- Depends on: 0189_microsoft_calendar_sync_state (the table + the policy
--             this one replaces), 0271_atoms_content_hash (the edit path
--             the delta items feed, `services::source_reingest`).
-- Down:
--   DROP POLICY IF EXISTS account_isolation ON microsoft_calendar_sync_state;
--   <re-create 0189's policy>
--   ALTER TABLE microsoft_calendar_sync_state
--       DROP COLUMN IF EXISTS delta_link,
--       DROP COLUMN IF EXISTS delta_resume_link,
--       DROP COLUMN IF EXISTS delta_window_end,
--       DROP COLUMN IF EXISTS delta_reconciled_at;

ALTER TABLE microsoft_calendar_sync_state
    ADD COLUMN IF NOT EXISTS delta_link          TEXT,
    ADD COLUMN IF NOT EXISTS delta_resume_link   TEXT,
    ADD COLUMN IF NOT EXISTS delta_window_end    TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS delta_reconciled_at TIMESTAMPTZ;

DROP POLICY IF EXISTS account_isolation ON microsoft_calendar_sync_state;
CREATE POLICY account_isolation ON microsoft_calendar_sync_state
    USING (account_id = current_setting('app.account_id')::uuid)
    WITH CHECK (
        account_id = current_setting('app.account_id')::uuid
        AND EXISTS (
            SELECT 1 FROM graphs
            WHERE graphs.id = microsoft_calendar_sync_state.graph_id
              AND graphs.account_id = current_setting('app.account_id')::uuid
              AND graphs.archived_at IS NULL
              AND (
                  -- Org plane (0189, unchanged): the member's own private
                  -- ocean, owned by exactly this row's owner_user_id.
                  (graphs.kind = 'member'
                   AND graphs.owner_user_id = microsoft_calendar_sync_state.owner_user_id)
                  -- Personal plane (this migration): the account's single
                  -- primary ocean, and the row's owner must be the account's
                  -- real owner — never any other user.
                  OR (graphs.kind = 'personal'
                      AND graphs.is_primary = true
                      AND microsoft_calendar_sync_state.owner_user_id = (
                          SELECT accounts.owner_user_id FROM accounts
                          WHERE accounts.id = microsoft_calendar_sync_state.account_id
                      ))
              )
        )
    );
