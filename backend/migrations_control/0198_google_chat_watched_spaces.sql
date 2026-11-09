-- Migration: 0198_google_chat_watched_spaces
-- Description: gsuite Train G4 PR 3 (G4.3) —
--   `.scratch/gsuite-enterprise/g4-chat-design.md` §3.1-§3.2, §5.2, §7 "G4.3".
--   The per-space destination table Chat needs, structurally the
--   `slack_watched_channels` (0101/0013) shape amended for two things Slack
--   never had: (a) an "unrouted" state a row can sit in BEFORE a human picks
--   a destination (D6: the Chat app being added to a space is itself the
--   observable authorising act, and creates this row immediately, in
--   isolation from whether anyone has since picked WHERE its messages go),
--   and (b) two archetypes sharing one table (`spaceType == SPACE` = the
--   Slack/container model; `GROUP_CHAT`/`DIRECT_MESSAGE` = the mailbox
--   model, owner-scoped, G4.5's opt-in).
--
--   ── Why a THIRD `mode` value ("unrouted", represented as NULL) ──
--   `slack_watched_channels`' `mode IN ('single','fanout')` CHECK assumes a
--   row exists only once a human has explicitly picked — absence of a row IS
--   "nobody has picked yet" there (see that migration's header). Chat cannot
--   reuse that convention: D6 requires a row to exist the moment
--   `ADDED_TO_SPACE` fires (`routes::google_push`'s new interaction-event
--   arm, this PR), independent of whether anyone has since picked a
--   destination — the row's EXISTENCE is what "the app is genuinely a
--   member of this space" durably records (docs/connectors/
--   CONNECTOR_CONTRACT.md §4, C2 = present and VISIBLE). So `mode` is
--   NULLable here: NULL = unrouted (created by `ADDED_TO_SPACE`, no
--   `graph_id`), 'single'/'fanout' = a human has since picked via
--   `POST /v1/connectors/google/chat/spaces/{space}/destination` (this PR's
--   route). The combined CHECK below widens `slack_watched_channels`'
--   mode/graph_id pairing with a third disjunct for exactly this state,
--   rather than inventing a sentinel `mode` string — NULL is the honest
--   "not yet decided" value SQL already has.
--
--   ── The archetype invariant (design doc §5.2) ──
--   `CHECK ((space_type = 'SPACE' AND owner_user_id IS NULL) OR (space_type
--   <> 'SPACE' AND mode = 'single' AND owner_user_id IS NOT NULL))`: a
--   `GROUP_CHAT`/`DIRECT_MESSAGE` row can only ever be 'single' (never
--   'fanout' — a mailbox has no "everyone" to fan out to; contract §7's
--   Mailbox archetype: owner's private ocean, no fan-out ever) and must
--   always name the opting-in member; a `SPACE` row can NEVER name an
--   owner (nobody "owns" a shared container — ownership is per-mailbox
--   only). This turns #672's "must branch and test on spaceType, not
--   inference" instruction from a code-review request into a DATABASE
--   invariant a bad write cannot get past regardless of which code path
--   attempts it — the same posture the design doc's §5.2 calls out
--   explicitly. (Security-review finding L4/L7, this PR round 2: the
--   ORIGINAL version of this CHECK was `space_type = 'SPACE' OR (...)`,
--   which SHORT-CIRCUITS true for any `SPACE` row regardless of
--   `owner_user_id` — a `SPACE` row with a non-NULL `owner_user_id` slipped
--   past it undetected, landing in the MAILBOX partial unique index
--   [`uq_google_chat_watched_spaces_mailbox`] and becoming invisible to
--   every query in this PR, which all filter on `owner_user_id IS NULL`
--   for the container archetype. No writer in this PR could ever produce
--   that shape, so this was latent, not exploited — tightened here so
--   G4.5's opt-in write path can't accidentally produce one either.)
--
--   ── Two disjoint partial unique indexes, not one composite UNIQUE ──
--   A `SPACE` row is unique per (account, space) — `owner_user_id IS NULL`
--   (nobody "owns" a shared container; ownership is per-mailbox only). A
--   `DIRECT_MESSAGE`/`GROUP_CHAT` row is unique per (account, space, owning
--   member) — TWO colleagues in the same account may each opt the SAME DM
--   into their OWN ocean independently (G4.5), which a single
--   `UNIQUE (account_id, space_id)` would wrongly collapse into one row.
--   Same `0175_oauth_tokens_owner_user_id.sql`-style disjoint-partial-index
--   pattern the design doc names (§5.2).
--
--   ── Columns reserved for later PRs in this train, unused by G4.3's own
--   code (documented here so a future reader doesn't have to re-derive
--   why they exist yet carry no writer today — same posture
--   `gmail_watched_threads.backfill_cursor`'s own header takes) ──
--   `reader_user_id` — G4.4's per-space designated-reader / failover
--   election (design doc §3.5, D8): this PR (G4.3) only ever sets it, at
--   `ADDED_TO_SPACE` time, to the adding member IF they are already
--   connected (`connectors::google_chat::record_added_to_space`) — it does
--   NOT re-elect on disconnect/403 (that failover logic is G4.4's).
--   `sync_token` / `backfill_cursor` / `backfill_complete` — G4.4's per-space
--   change-detection cursors (design doc §4.4). `backfill_complete` defaults
--   TRUE (matches `gmail_watched_threads`' own convention: a row with no
--   backfill machinery reads as "nothing pending" rather than "perpetually
--   incomplete").
--   `last_ingested_at` — bumped by G4.4's ingest funnel; nullable (never
--   ingested yet, unlike `last_resolved_at` which THIS PR's routes already
--   bump on every read).
--
--   ── history_state / active ──
--   `history_state` — `Space.historyState` captured at `ADDED_TO_SPACE`
--   time (`spaces.get`, §2.2) and re-read (best-effort refresh) whenever the
--   destination route resolves the space, so the `HISTORY_OFF` disclosure
--   (§5.4) is never more than one resolution stale. Empty string (never
--   NULL) when unknown, same "unused/not-yet-known is the empty string"
--   convention `slack_watched_channels.display_name` documents.
--   `active` — D6/§3.5: `REMOVED_FROM_SPACE` (this PR's `routes::
--   google_push` arm, fanned out by `resolve_chat_space_account` to every
--   account holding a row — see that function's own header, added post-
--   security-review) sets this FALSE and stops ingest immediately (Slack
--   #502's lesson: disconnect actually stopping ingest). A re-add
--   (`ADDED_TO_SPACE` for a space this account already has a row for) flips
--   it back TRUE — but, per FOUNDER DECISION G4-Q6 (2026-08-25, recorded in
--   `.scratch/gsuite-enterprise/plan.md`), a re-add from an INACTIVE row
--   ALSO resets `mode`/`graph_id` back to NULL (unrouted) rather than
--   resurrecting the prior destination: the space's roster may have turned
--   over completely while the app was removed, so resuming silently would
--   make a new, un-consulted set of people inherit a consent they never
--   gave. Contract I2 ("a destination is fixed at authorisation time") is
--   satisfied by treating the RE-ADD as a fresh authorisation opportunity,
--   not by treating the OLD pick as still valid across a removal. The
--   lifecycle is therefore: add → unrouted → routed → removed (deactivated)
--   → re-add → unrouted AGAIN — a loop back to "needs a human to pick",
--   never straight back to routed. A REDUNDANT `ADDED_TO_SPACE` on a row
--   that was already `active` (no removal happened — a duplicate
--   delivery) is the one case that preserves an existing pick, since there
--   was no removal to have invalidated it.
--
--   ── RLS: `slack_watched_channels`' stricter-WITH-CHECK-than-USING shape
--   is the model, not `gmail_watched_threads`' USING-only gap ──
--   #692's round-1 finding, carried forward here per the design doc (§1.8):
--   `gmail_watched_threads`' consumer-plane policy is `USING`-only, with no
--   explicit re-assertion that a WRITE's `graph_id` actually belongs to the
--   writing account — closed for `slack_watched_channels` (0101) by an
--   EXPLICIT `WITH CHECK` re-deriving both `account_id` AND `graph_id`
--   ownership, NULL-friendly (`graph_id IS NULL OR graph_id IN (...)`) so a
--   legitimate NULL-`graph_id` row ('fanout', or here, 'unrouted' too)
--   isn't rejected by SQL's three-valued-logic trap (`x IN (...)` evaluates
--   NULL, not TRUE, when `x` IS NULL). Ships from THIS migration, not
--   retrofitted later.
--
-- Depends on: 0001_init (accounts, users), 0005 (graphs),
--             0101_slack_watched_channels (the reference shape + its WITH
--             CHECK fix this migration starts from), 0083_consumer_rls
--             (coralswarm_app role + default privileges — no fresh GRANT
--             needed here, same as 0101).
--
-- Paired with: migrations_tenant/0073_google_chat_watched_spaces.sql — a
--             shared content table, so adding it to only one tree fails
--             scripts/check_schema_parity.sh by design.
--
-- Down:
--   REVOKE EXECUTE ON FUNCTION resolve_chat_space_account(text) FROM coralswarm_app, CURRENT_USER;
--   REVOKE EXECUTE ON FUNCTION resolve_chat_space_account(text) FROM PUBLIC;
--   DROP FUNCTION IF EXISTS resolve_chat_space_account(text);
--   REVOKE EXECUTE ON FUNCTION resolve_google_identity_by_sub(text) FROM coralswarm_app, CURRENT_USER;
--   REVOKE EXECUTE ON FUNCTION resolve_google_identity_by_sub(text) FROM PUBLIC;
--   DROP FUNCTION IF EXISTS resolve_google_identity_by_sub(text);
--   DROP POLICY IF EXISTS account_isolation ON google_chat_watched_spaces;
--   ALTER TABLE google_chat_watched_spaces DISABLE ROW LEVEL SECURITY;
--   DROP TABLE IF EXISTS google_chat_watched_spaces;

CREATE TABLE IF NOT EXISTS google_chat_watched_spaces (
    id                 UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id         UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    -- "spaces/{id}" — Chat's stable, globally unique space resource name.
    space_id           TEXT        NOT NULL,
    space_type         TEXT        NOT NULL
                                    CHECK (space_type IN ('SPACE', 'GROUP_CHAT', 'DIRECT_MESSAGE')),
    -- NULL = unrouted (see this migration's header). 'single'/'fanout' once
    -- a human has picked via the destination route.
    mode               TEXT        CHECK (mode IS NULL OR mode IN ('single', 'fanout')),
    -- Destination ocean. NULL when mode IS NULL or mode = 'fanout'.
    graph_id           UUID        REFERENCES graphs(id) ON DELETE CASCADE,
    -- The opting-in member for a DIRECT_MESSAGE/GROUP_CHAT row (G4.5). NULL
    -- for a SPACE row — nobody "owns" a shared container.
    owner_user_id      UUID        REFERENCES users(id) ON DELETE CASCADE,
    -- Reserved for G4.4's per-space designated-reader/failover election
    -- (design doc §3.5, D8) — see this migration's header.
    reader_user_id     UUID        REFERENCES users(id) ON DELETE CASCADE,
    display_name       TEXT        NOT NULL DEFAULT '',
    -- Space.historyState ('HISTORY_ON'/'HISTORY_OFF'/'' unknown) — §2.2/§5.4.
    history_state      TEXT        NOT NULL DEFAULT '',
    -- FALSE once REMOVED_FROM_SPACE fires, or (G4.4) once the last connected
    -- reader is gone (§3.5's "nobody left" end state). Ingest checks this.
    active             BOOLEAN     NOT NULL DEFAULT TRUE,
    -- Reserved for G4.4 (design doc §4.4) — unused by this PR's own code.
    sync_token         TEXT,
    backfill_cursor    TEXT,
    backfill_complete  BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Bumped on every destination RESOLUTION (this PR's GET/POST routes),
    -- success or not-yet-routed alike — same "resolution time, not ingest
    -- time" convention `slack_watched_channels.last_resolved_at` documents.
    last_resolved_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Bumped by G4.4's ingest funnel; NULL until the first successful
    -- ingest, unlike last_resolved_at which this PR already bumps.
    last_ingested_at   TIMESTAMPTZ,

    -- Ties mode to graph_id's nullability, WIDENED from
    -- slack_watched_channels' two-way CHECK to admit the third "unrouted"
    -- state (mode IS NULL) — see this migration's header.
    CONSTRAINT google_chat_watched_spaces_mode_graph_id_check CHECK (
        (mode = 'single' AND graph_id IS NOT NULL)
        OR (mode = 'fanout' AND graph_id IS NULL)
        OR (mode IS NULL AND graph_id IS NULL)
    ),
    -- The archetype invariant (design doc §5.2) — see this migration's
    -- header, including the L4/L7 tightening: a SPACE row can never name
    -- an owner; a GROUP_CHAT/DIRECT_MESSAGE row can never be 'fanout' or
    -- unrouted, and always names its owner.
    CONSTRAINT google_chat_watched_spaces_archetype_check CHECK (
        (space_type = 'SPACE' AND owner_user_id IS NULL)
        OR (space_type <> 'SPACE' AND mode = 'single' AND owner_user_id IS NOT NULL)
    )
);

-- Two disjoint partial uniques (design doc §5.2) — see this migration's
-- header for why one composite UNIQUE cannot represent both archetypes.
CREATE UNIQUE INDEX IF NOT EXISTS uq_google_chat_watched_spaces_container
    ON google_chat_watched_spaces (account_id, space_id)
    WHERE owner_user_id IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_google_chat_watched_spaces_mailbox
    ON google_chat_watched_spaces (account_id, space_id, owner_user_id)
    WHERE owner_user_id IS NOT NULL;

ALTER TABLE google_chat_watched_spaces ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON google_chat_watched_spaces;
-- READ: account_id alone, direct and unambiguous. WRITE: additionally
-- requires graph_id (when set) to belong to a graph owned by that SAME
-- account — slack_watched_channels' fix for the bare-USING-reused-as-
-- WITH-CHECK class, applied here from the start (see header).
CREATE POLICY account_isolation ON google_chat_watched_spaces
    USING (account_id = current_setting('app.account_id')::uuid)
    WITH CHECK (
        account_id = current_setting('app.account_id')::uuid
        AND (
            -- NULL-friendly (unrouted or fanout rows carry a NULL graph_id;
            -- `x IN (...)` is NULL, not TRUE, for a NULL x) — not a
            -- weakening: the CHECK constraints above already guarantee a
            -- 'single' row's graph_id is never NULL.
            graph_id IS NULL
            OR graph_id IN (
                SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
            )
        )
    );

-- ── resolve_google_identity_by_sub(text) — control-only SECURITY DEFINER
-- bootstrap resolver (design doc §3.3, §1.8's "the touchpoints" table) ──
--
-- routes::google_push's Chat-app-interaction-event arm (this PR) receives
-- ADDED_TO_SPACE/REMOVED_FROM_SPACE events carrying only the acting user's
-- Google `sub` (as "users/{id}") — no account_id, no owner_user_id. Same
-- bootstrap problem `resolve_gmail_watch_owner` (migration 0187) solves for
-- Gmail's push path, keyed differently: by `oauth_tokens.provider_user_id`
-- rather than a watch-channel resource id, and identification-only (never a
-- credential read) per design doc §3.3's "provider LIKE 'google%', never a
-- credential lookup" rule — this seats/resolves WHO a person is, not WHICH
-- token to load. `provider LIKE 'google%'` (not `= 'google_chat'`)
-- deliberately: a colleague who has connected Gmail/Meet but not yet Chat is
-- still a real, identifiable CoralSwarm member, and refusing to resolve them
-- here would be I4 read backwards (design doc §3.3's exact argument).
--
-- Fails closed on cross-account ambiguity as resolve_gmail_watch_owner
-- does — SAME email/sub connected under two different CoralSwarm accounts
-- resolves to ZERO rows (not an arbitrary pick), never a cross-account
-- misroute of who gets to add/remove a watched space.
--
-- ── SCOPE, tightened post-security-review (M3, this PR round 2): the
-- ADDED_TO_SPACE bootstrap ONLY, never routing, never seating ──
-- Design doc §3.3 specifies an ACCOUNT-SCOPED seating/identity helper
-- (`resolve_google_identity_user_id`, `WHERE account_id = $1`) and §3.5
-- states flatly that "no `sub`-keyed resolver exists at all" for routing —
-- "the `sub` join survives for seating and authorship only — never for
-- routing." This CROSS-ACCOUNT function exists ONLY because
-- `ADDED_TO_SPACE`'s bootstrap problem is genuinely account-DISCOVERY (no
-- `account_id` is known yet — that's the whole point), a case the design
-- didn't consider when it wrote "no sub-keyed resolver, ever." Every OTHER
-- caller that already knows its `account_id` (seating counts, the
-- destination-pick disclosure) MUST use the account-scoped, non-SECURITY-
-- DEFINER query design doc §3.3 actually specifies instead
-- (`connectors::google_chat::resolve_google_identity_in_account`, run
-- inside `begin_account_scoped`) — using THIS function there was this PR's
-- original M3 defect: it silently counted a roster member as "matched" if
-- their `sub` resolved under ANY CoralSwarm account, not specifically the
-- account routing THIS space, over-stating contract §5.4's mandatory
-- seating-count disclosure.
CREATE OR REPLACE FUNCTION resolve_google_identity_by_sub(p_sub text)
RETURNS TABLE(account_id uuid, owner_user_id uuid)
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    WITH candidates AS (
        SELECT ot.account_id, ot.owner_user_id
        FROM oauth_tokens ot
        WHERE ot.provider LIKE 'google%'
          AND ot.provider_user_id = p_sub
          AND p_sub <> ''
          AND ot.provider_user_id <> ''
          AND ot.owner_user_id IS NOT NULL
    )
    SELECT c.account_id, c.owner_user_id
    FROM candidates c
    WHERE (SELECT count(DISTINCT c2.account_id) FROM candidates c2) = 1
    LIMIT 1
$$;

REVOKE EXECUTE ON FUNCTION resolve_google_identity_by_sub(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION resolve_google_identity_by_sub(text) TO coralswarm_app, CURRENT_USER;

-- ── resolve_chat_space_account(text) — control-only SECURITY DEFINER,
-- the SPACE-keyed resolver design doc §3.5 (D8) asks for ──
--
-- Added post-security-review (H1, this PR round 2). `REMOVED_FROM_SPACE`
-- is an event about the SPACE (the Chat app was removed from it) — design
-- doc §3.5 is explicit that routing/deactivation must key on "the space
-- resource name … never the subscriber" (i.e. never the acting/removing
-- member). The original version of `routes::google_push::
-- handle_chat_removed_from_space` resolved the ACCOUNT from the REMOVING
-- member's own identity via `resolve_google_identity_by_sub` and
-- deactivated only THAT account's row — so a removal performed by anyone
-- OTHER than the member who originally routed the space (the common case:
-- most people who can remove a Workspace-installed Chat app from a space
-- have never connected CoralSwarm at all) resolved to no known identity,
-- and the row that was actually feeding an ocean was NEVER deactivated —
-- directly contradicting this table's own header ("REMOVED_FROM_SPACE …
-- stops ingest immediately") and `docs/connectors/google-workspace.md`.
--
-- Returns a SET, not a single row: unlike `resolve_gmail_watch_owner`/
-- `resolve_google_identity_by_sub` (which each resolve WHO gets credited
-- for an ambiguous identity and must fail closed to avoid a wrong pick),
-- there is no "wrong" account to act on here — the physical Chat space was
-- removed from, full stop, and EVERY CoralSwarm account independently
-- holding a container row for it (the unique index is `(account_id,
-- space_id)`, not globally unique on `space_id` alone — two accounts CAN
-- each hold their own row for the same underlying space) must have its row
-- deactivated. Fan-out-and-deactivate-each is the correct response to
-- ambiguity here, the opposite of fail-closed.
--
-- Reads `google_chat_watched_spaces` directly rather than joining through
-- an intermediate identity — correct today because G4.3 is Personal-plane-
-- only (D1): every account capable of holding a row has its content in
-- THIS SAME control database (Personal-plane content == control-plane
-- content). Once G4.6 lifts the org gate, an org account's rows live in
-- its own separate tenant database, invisible to this control-only
-- function — the same Personal-plane-only limitation
-- `routes::google_push::run_gmail_sync_for_email`'s own doc comment
-- documents for Gmail's push path; G4.6 must revisit this function, not
-- silently inherit a blind spot.
CREATE OR REPLACE FUNCTION resolve_chat_space_account(p_space text)
RETURNS SETOF uuid
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT DISTINCT gcws.account_id
    FROM google_chat_watched_spaces gcws
    WHERE gcws.space_id = p_space
      AND p_space <> ''
      AND gcws.owner_user_id IS NULL
$$;

REVOKE EXECUTE ON FUNCTION resolve_chat_space_account(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION resolve_chat_space_account(text) TO coralswarm_app, CURRENT_USER;
