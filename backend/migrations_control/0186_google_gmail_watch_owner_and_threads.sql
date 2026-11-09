-- Migration 0186: per-member Gmail watch ownership + opt-in thread tracking
--   (gsuite Train G2 PR 3, `.scratch/gsuite-enterprise/g2-design.md` PR G2.3).
--
--   RENUMBERED from the originally-announced 0185 (control) after #683
--   merged holding that number for `coding_sessions.session_kind` — merged
--   wins; the ledger was updated to hand this PR 0186 (control) / 0066
--   (tenant twin, see `migrations_tenant/
--   0066_google_gmail_watch_owner_and_threads.sql`) / 0187 (control-only
--   `resolve_gmail_watch_owner_function.sql`) instead. Purely a rename —
--   no content change.
--
--   Two independent pieces, announced together in the m365 migration ledger
--   as the next free control number (0186, after this renumber) following
--   #680's 0184 and #683's 0185:
--
--   ── 1. `google_watch_channels.owner_user_id` ──
--   `migrations_control/0180_google_watch_channels.sql` has no member
--   dimension — every row (Calendar, and Chat/Meet whenever they land) has
--   been account-wide so far. Gmail (gsuite Train G2 PR 1's per-surface
--   token isolation, `connectors::google::Surface::token_owner_for`) is the
--   first surface with genuinely per-member OAuth storage, so its watch rows
--   need the same dimension: one `google_watch_channels` row per connected
--   member's mailbox (`surface='gmail', resource_id=<member's email>`),
--   `owner_user_id` naming which CoralSwarm user that connection belongs to.
--   NULL keeps meaning "account-wide" (Calendar's existing rows, and any
--   future account-wide surface) — nullable, no backfill, existing rows
--   untouched. `ON DELETE CASCADE`, not `SET NULL`: a NULL owner_user_id is
--   a live, meaningful value ("account-wide") elsewhere in this same column,
--   so a deleted user's Gmail watch row must be removed outright, never
--   silently reinterpreted as "account-wide" by a `SET NULL`.
--
--   ── 2. `gmail_watched_threads` ──
--   Structural copy of `outlook_watched_conversations`
--   (`migrations_control/0035_outlook_thread_watch.sql`) plus
--   `0179_graph_backfill_cursors.sql`'s `backfill_cursor`/`backfill_complete`
--   pair (`DEFAULT TRUE`, same "existing/synchronous inserts need no
--   surprise re-backfill" reasoning that migration gives) — the Gmail
--   analogue of "the set of threads a user explicitly opted in, so an
--   inbound history.list diff can be matched against it and everything else
--   discarded" (ADR-0008 storage-scoping, ported from Outlook's identical
--   mailbox-wide-subscription-but-storage-scoped-to-opt-in shape). See
--   `connectors::google_gmail`'s module doc for the full ingest design;
--   `backfill_cursor`/`backfill_complete` are carried for schema-parity with
--   the Outlook precedent and reserved for Train G2.4's everything-mode
--   resumable backfill — G2.3's own opt-in path (`threads.get(format=full)`,
--   no pagination) always writes `backfill_complete = TRUE` synchronously,
--   the same "ran fully synchronously, no crash window to protect" reasoning
--   `microsoft.rs::after_ingest`'s own comment gives for its
--   `outlook_watched_conversations` row.
--
--   RLS: control `account_isolation` (mirrors `outlook_watched_conversations`
--   exactly — anchored on `account_id` directly, same shape as every other
--   0083-class table); the tenant twin (`migrations_tenant/
--   0066_google_gmail_watch_owner_and_threads.sql`) carries `member_ocean_
--   isolation` anchored on `graph_id`, so this table lands in all four
--   rosters `outlook_watched_conversations` already lives in (design doc
--   §1.6): `scripts/check_schema_parity.sh`'s `CONTROL_ONLY_RLS_TABLES` +
--   `TENANT_ONLY_RLS_TABLES`, `backend/tests/consumer_rls_pg.rs`'s
--   `RLS_TABLES`, `backend/tests/tenant_rls_pg.rs`'s `RLS_TABLES`.
--
--   ── No standalone `(account_id)` index (CodeRabbit round 1 finding #10 on
--   PR #686, trivial) ──
--   `outlook_watched_conversations` (0035) carries one
--   (`idx_watched_conversations_account`) alongside its own `UNIQUE
--   (account_id, conversation_id)`, with a comment claiming it "serves the
--   renewal/cleanup scans by account" — but nothing in this codebase
--   actually issues a bare `WHERE account_id = $1` scan against either
--   table (every reader here — `watched_thread_ids_among`/
--   `watched_thread_graph_ids`, `connectors::google_gmail` — always filters
--   `account_id` together with `thread_id`, which the leading column of the
--   `UNIQUE` constraint's own index already serves). A standalone
--   single-column index whose leading column duplicates a composite unique
--   index's leading column buys Postgres nothing a real query here needs,
--   so — noting the Outlook precedent exists, but finding no measured
--   reason for it either — this migration does not replicate it. Both
--   migrations are unmerged, so this is a same-PR correction, not a
--   follow-up.
--
-- Depends on: 0180_google_watch_channels.sql, 0035_outlook_thread_watch.sql (precedent).
-- Down:
--   DROP INDEX IF EXISTS idx_gmail_watched_threads_backfill_pending;
--   DROP TABLE IF EXISTS gmail_watched_threads;
--   ALTER TABLE google_watch_channels DROP COLUMN IF EXISTS owner_user_id;

ALTER TABLE google_watch_channels
    ADD COLUMN IF NOT EXISTS owner_user_id UUID REFERENCES users(id) ON DELETE CASCADE;

CREATE TABLE IF NOT EXISTS gmail_watched_threads (
    id                 UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id         UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    -- Destination ocean for auto-captured replies (the ocean the thread was
    -- first opted into) — same role as
    -- `outlook_watched_conversations.graph_id`.
    graph_id           UUID        NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
    -- The member who opted the thread in; auto-captured replies are ingested
    -- as this user (reachability + provenance — notifications carry no caller).
    ingested_by        UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    -- Gmail's stable thread id (constant across every message in the thread).
    thread_id          TEXT        NOT NULL,
    -- Reserved for Train G2.4's everything-mode resumable backfill — unused
    -- (always NULL / TRUE) by G2.3's own opt-in path, see header above.
    backfill_cursor    TEXT,
    backfill_complete  BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (account_id, thread_id)
);

-- Notification matching is by (account_id, thread_id) — the UNIQUE
-- constraint's own index already serves it (leading column account_id); see
-- this migration's header for why no standalone (account_id) index is
-- added, unlike 0035's outlook precedent.
CREATE INDEX IF NOT EXISTS idx_gmail_watched_threads_backfill_pending
    ON gmail_watched_threads (account_id)
    WHERE NOT backfill_complete;

ALTER TABLE gmail_watched_threads ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON gmail_watched_threads;
CREATE POLICY account_isolation ON gmail_watched_threads
    USING (account_id = current_setting('app.account_id')::uuid);
