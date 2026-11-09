-- Migration: 0101_slack_watched_channels
-- Description: docs/connectors/slack.md §2/§4 — the per-channel destination
--   table Slack has never had (contrast Teams' `teams_watched_resources`,
--   migration 0037). One row per `(account_id, workspace_id, channel_id)`:
--   the destination ocean a channel's messages ingest into, chosen
--   EXPLICITLY by a human via the (not-yet-built) destination picker
--   (docs/connectors/slack.md §2's three-option picker: everyone's own
--   ocean / an existing team ocean / a new channel-mirrored ocean), and read
--   by the message-ingest funnel instead of resolving a primary ocean at
--   ingest time.
--
--   Why this exists now. Every Slack ingest path currently passes
--   `IngestCaller::NonAttributable` into `routes::slack::resolve_primary_graph`
--   (`routes/slack_events.rs::ingest_slack_message`). Under a PERSONAL account
--   that resolves fine (the personal branch ignores the caller entirely) —
--   but under an ORG account it REFUSES unconditionally (the org branch's
--   `IngestCaller::NonAttributable` arm), because there is no attributable
--   session behind a Slack Events API webhook. So Slack ingest has been
--   fail-closed dead for every org account since the org gate landed. Reading
--   a per-channel destination here removes `resolve_primary_graph` from the
--   steady-state ingest path entirely for personal accounts and for org
--   accounts that HAVE made an explicit choice — see `routes/slack_events.rs`
--   for the read side (`connectors::slack::resolve_channel_destinations`).
--
--   ── This table has NO write side yet ──
--   Unlike an earlier draft of this migration, `handle_member_joined` (the
--   bot's self-join handler) does NOT write a row here. A guessed default —
--   "pick an arbitrary representative account member and treat their ocean
--   as this channel's destination, forever, because contract I2 says the
--   destination is fixed at authorisation time" — is not an authorisation;
--   nobody chose it. Persisting a guess as if it were a deliberate pick is
--   worse than not persisting anything: it freezes a wrong default in place
--   (I2) instead of letting the resolution improve as identity mapping
--   (capability C5) gets wired up. So: this table is populated ONLY by the
--   future interactive picker (a separate, later task). Until that ships it
--   is expected to be EMPTY, and every channel resolves through the
--   account-level default instead
--   (`connectors::slack::resolve_channel_destinations`): a personal
--   account's own primary ocean (unchanged), or — for an org account — a
--   live per-member fan-out that seats only members an identity mapping can
--   actually confirm, surfacing an "N members unmapped" count rather than
--   guessing (docs/connectors/CONNECTOR_CONTRACT.md §4, capability C5 row).
--
--   ── AMENDED BEFORE SHIPPING: `mode` discriminator + nullable `graph_id` ──
--   This migration has never shipped (it exists only on an unpushed
--   worktree branch), so this is an in-place edit, not a follow-on
--   migration — see `scripts/check_migrations.sh`'s append-only guard,
--   which only bites a migration once it is reachable from `main`.
--
--   The write side above landed the interactive picker's persistence
--   primitive (`connectors::slack::ensure_slack_watched_channel`) before the
--   picker's UI did, and the picker's first org-plane option is "everyone's
--   individual oceans" — a fan-out pick with NO single destination ocean to
--   store. The original shape (`graph_id UUID NOT NULL`) had no way to
--   represent that pick at all: a channel with no row and a channel whose
--   human explicitly chose fan-out both read as "nobody has picked yet",
--   which is fine ONLY while the account-level default also degrades to
--   fan-out — the moment that default changes (e.g. once capability C5
--   ships and the default becomes "seat everyone"), an explicit fan-out
--   pick silently reinterprets as an unmade decision. Absence of a row must
--   never be allowed to mean two different things.
--
--   `mode TEXT NOT NULL DEFAULT 'single' CHECK (mode IN ('single','fanout'))`
--   plus widening `graph_id` to nullable, tied together by a table-level
--   CHECK `(mode='single' AND graph_id IS NOT NULL) OR (mode='fanout' AND
--   graph_id IS NULL)`, so the two columns can never drift apart: a
--   'single' row always names its ocean, a 'fanout' row never does, and no
--   third combination can exist. `DEFAULT 'single'` keeps every row this
--   migration's own tests already seed (which never mention `mode`)
--   unchanged.
--
--   ── Shape, copied from `teams_watched_resources` (0037/0038/0047) ──
--   `id` — surrogate PK, no code currently keys off it directly (kept for
--   parity with the reference shape and because a bare
--   `(account_id, workspace_id, channel_id)` composite PK would make the
--   FK-cascade / ON CONFLICT shapes below noisier than they need to be).
--   `account_id` — FK `accounts(id) ON DELETE CASCADE`: the CoralSwarm
--   account this Slack workspace is connected under (same table
--   `slack_integrations`/`oauth_tokens` key on).
--   `user_id` — FK `users(id) ON DELETE CASCADE`. Provenance/bookkeeping
--   ONLY, exactly like `teams_watched_resources.user_id`'s doc comment: the
--   human who made the explicit pick via the future picker — NOT
--   necessarily the one who ran `/v1/integrations/slack/oauth/start` or the
--   one who actually invited the bot into this channel.
--   `graph_id` — FK `graphs(id) ON DELETE CASCADE`: the destination ocean.
--   Unlike `teams_watched_resources` (which distinguishes `kind IN
--   ('channel','chat')` with three optional id columns), Slack has exactly
--   one container shape (channel), so there is no `kind` column and no
--   unused-id-column convention to apply here.
--   `workspace_id` / `channel_id` — TEXT NOT NULL (never nullable — a Slack
--   channel is always identified by both together; contrast Teams' `chat_id`/
--   `channel_id` split, which Slack has no need of).
--   `display_name` — TEXT NOT NULL DEFAULT '' — the human label for a future
--   management UI (`#general`, etc.), matching the "unused/not-yet-known
--   value is the empty string, never NULL" convention from the reference
--   shape's own header note.
--   `created_at` / `last_resolved_at` — same bookkeeping pair as
--   `teams_watched_resources`'s `last_ingested_at`, renamed here (this table
--   has never shipped, so the rename is free): the read side bumps this
--   column at DESTINATION-RESOLUTION time (see `slack_watched_destination`),
--   which happens on every trigger — including a skip, and including a
--   retry — not only on a successful ingest. `last_ingested_at` would claim
--   something this column has never actually meant; `last_resolved_at` says
--   what it really tracks. No code reads this column today (zero readers),
--   so this is the free half of the fix; a future "this channel has gone
--   quiet" health surface must not be built on the assumption that a recent
--   bump means a recent successful ingest.
--
--   ── RLS: this table is NOT the `slack_integrations`/`slack_huddle_captures`
--   precedent ──
--   Both of those sit in the tenant baseline today but carry NO tenant RLS
--   policy at all (`migrations_tenant/0009_tenant_rls.sql`'s
--   `DELIBERATELY_UNCOVERED` roster) — they are inert on the org/tenant
--   plane, which is exactly the bug this table exists to help fix for message
--   ingest. `slack_watched_channels` is deliberately NOT copied onto that
--   inert list: it IS meant to work on the org plane (an org's tenant
--   database is where its `graphs`/per-member oceans actually live), so it
--   gets the SAME two-plane RLS treatment as `teams_watched_resources` —
--   consumer-plane `account_isolation` here (added below, in THIS migration,
--   since the table did not exist when 0083_consumer_rls ran) and
--   tenant-plane `member_ocean_isolation` anchored on `graph_id` in the
--   paired `migrations_tenant/0013_slack_watched_channels.sql`. Both new
--   tables/policies must be registered in `scripts/check_schema_parity.sh`'s
--   `CONTROL_ONLY_RLS_TABLES`/`TENANT_ONLY_RLS_TABLES` (done, same PR) and in
--   `backend/tests/consumer_rls_pg.rs` / `tenant_rls_pg.rs`'s `RLS_TABLES`
--   (done, same PR) — the roster-assertion tests in both files fail loudly if
--   a table carrying RLS is left off, so this is enforced, not just
--   documented.
--
--   ── RLS predicate: `account_id` for READ, `account_id` AND `graph_id`
--   ownership for WRITE ──
--   This table has BOTH a direct `account_id` column AND a `graph_id` column
--   one hop from `graphs`. `USING (account_id = current_setting(...))` alone
--   is a correct and complete READ predicate — `account_id` is a single,
--   direct, unambiguous anchor, unlike `node_promotions`' two-INDEPENDENT-
--   anchors case (0083's `node_promotions` block) where either anchor alone
--   makes a row visible. The bug here is narrower and different: a bare
--   `USING`-only policy is `FOR ALL` and reuses `USING` for `WITH CHECK`
--   too, which checks `account_id` matches but says NOTHING about whether
--   the row's `graph_id` also belongs to that same account. Without an
--   explicit `WITH CHECK`, account A could INSERT/UPDATE a row with its own
--   `account_id` but a `graph_id` FK-referencing account B's graph — the FK
--   alone permits this (`graph_id UUID NOT NULL REFERENCES graphs(id)`, no
--   account correlation), and `connectors::slack::slack_watched_destination`
--   would then hand that foreign graph back to
--   `routes/slack_events.rs::ingest_slack_message`, which writes atoms into
--   it — a real cross-account write. So this table needs an explicit
--   `WITH CHECK` (below) that both re-asserts `account_id` and verifies
--   `graph_id`'s ownership via a subquery back to `graphs` — but, unlike
--   `node_promotions`, does NOT need the 4-policy READ/WRITE split: there is
--   only one anchor for READ (`account_id`, direct, unambiguous), so a
--   single `FOR ALL` policy with a stricter `WITH CHECK` than its `USING` is
--   sufficient and keeps `scripts/check_schema_parity.sh`'s generic
--   `DROP POLICY IF EXISTS account_isolation ON $t` handling (rather than the
--   `node_promotions`-shaped named-policy special case) working unchanged.
--
--   ── `graph_id IS NULL OR graph_id IN (...)` — NULL-friendliness, not a
--   weakening ──
--   The `mode`/nullable-`graph_id` amendment above means `graph_id` can now
--   legitimately be NULL (a 'fanout' row). `x IN (subquery)` is NULL-
--   UNFRIENDLY: when `x` is NULL the whole expression evaluates to NULL, not
--   TRUE, so a bare `graph_id IN (...)` WITH CHECK would REJECT every
--   legitimate fanout insert — a real regression this amendment must not
--   introduce. `graph_id IS NULL OR graph_id IN (...)` fixes that without
--   opening a hole: the table-level CHECK above already guarantees a
--   'single' row's `graph_id` is never NULL, so the `IS NULL` arm can only
--   ever be satisfied by a genuine 'fanout' row — there is no `mode` value
--   that lets a 'single' row smuggle a foreign `graph_id` through as NULL.
--   Cross-account protection for 'single' rows is exactly as strict as
--   before. See `backend/tests/consumer_rls_pg.rs` for both the pre-existing
--   cross-account-graph_id proof (§4c) and its NULL-graph_id companion.
--
--   `coralswarm_app` already has `SELECT, INSERT, UPDATE, DELETE` on this new
--   table via 0083's `ALTER DEFAULT PRIVILEGES` (no fresh GRANT needed here).
--
-- Depends on: 0001_init (accounts, users), 0005 (graphs), 0037/0038/0047
--             (the teams_watched_resources reference shape this mirrors),
--             0083_consumer_rls (coralswarm_app role + default privileges).
--
-- Paired with: migrations_tenant/0013_slack_watched_channels.sql — this is
--             one of the shared content tables, so adding it to only one
--             tree fails `scripts/check_schema_parity.sh` by design.
--
-- Down:
--   DROP POLICY IF EXISTS account_isolation ON slack_watched_channels;
--   ALTER TABLE slack_watched_channels DISABLE ROW LEVEL SECURITY;
--   DROP TABLE IF EXISTS slack_watched_channels;

CREATE TABLE IF NOT EXISTS slack_watched_channels (
    id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id       UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    -- Provenance only — see this migration's header. The human who made the
    -- explicit pick via the (future) destination picker.
    user_id          UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    -- Mode discriminator — see the "AMENDED BEFORE SHIPPING" header section.
    -- 'single': graph_id names the one destination ocean. 'fanout': an
    -- EXPLICIT "everyone's individual oceans" pick with no single ocean to
    -- store.
    mode             TEXT        NOT NULL DEFAULT 'single'
                                 CHECK (mode IN ('single', 'fanout')),
    -- Destination ocean, chosen explicitly (see this migration's header —
    -- there is no write side yet, so this table is empty until the picker
    -- ships). NULL exactly when mode = 'fanout' — enforced by the CHECK
    -- below, never by convention alone.
    graph_id         UUID        REFERENCES graphs(id) ON DELETE CASCADE,
    workspace_id     TEXT        NOT NULL,
    channel_id       TEXT        NOT NULL,
    -- Human label for a future management UI. Empty string (not NULL) when
    -- unresolved, matching the reference shape's unused-id convention.
    display_name     TEXT        NOT NULL DEFAULT '',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Bumped at destination-RESOLUTION time (every trigger, success or skip
    -- or retry) — see this migration's header. Renamed from
    -- `last_ingested_at` (the `teams_watched_resources` reference shape's
    -- spelling) because this table never shipped under that name; no code
    -- reads this column today.
    last_resolved_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- One destination per channel per account (contract I2: one container,
    -- one destination, fixed at authorisation time).
    UNIQUE (account_id, workspace_id, channel_id),

    -- Ties `mode` to `graph_id`'s nullability so the two columns can never
    -- drift apart — see the "AMENDED BEFORE SHIPPING" header section.
    CONSTRAINT slack_watched_channels_mode_graph_id_check CHECK (
        (mode = 'single' AND graph_id IS NOT NULL)
        OR (mode = 'fanout' AND graph_id IS NULL)
    )
);

-- The message-ingest funnel's lookup shape exactly
-- (`WHERE account_id = $1 AND workspace_id = $2 AND channel_id = $3`); the
-- UNIQUE constraint above already creates this composite index, so no
-- separate CREATE INDEX is needed.

ALTER TABLE slack_watched_channels ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON slack_watched_channels;
-- READ: `account_id` alone is a correct, direct, unambiguous anchor.
-- WRITE: additionally requires `graph_id` to belong to a graph owned by that
-- SAME account — closes the cross-account-graph-reference bug this
-- migration's header describes (this repo's second occurrence of the
-- bare-USING-reused-as-WITH-CHECK class; see 0083's `node_promotions` block
-- for the first).
CREATE POLICY account_isolation ON slack_watched_channels
    USING (account_id = current_setting('app.account_id')::uuid)
    WITH CHECK (
        account_id = current_setting('app.account_id')::uuid
        AND (
            -- NULL-friendly, not weaker — see this migration's
            -- "NULL-friendliness, not a weakening" header note. A 'fanout'
            -- row's graph_id is NULL by the table CHECK above, so there is
            -- nothing to verify ownership of; a 'single' row can never
            -- reach this arm with a NULL graph_id.
            graph_id IS NULL
            OR graph_id IN (
                SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
            )
        )
    );
