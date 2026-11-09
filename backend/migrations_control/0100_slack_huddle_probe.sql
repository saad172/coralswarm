-- Slack huddle event probe — TEMPORARY DIAGNOSTIC, delete with the probe.
--
-- `docs/connectors/slack.md` §3.1 and §5 flag "which Slack event signals the
-- notes canvas" as a build blocker and say to verify rather than guess, because
-- guessing wrong means huddles silently never get captured. Slack documents
-- none of the `room` object, so the only way to settle it is to read a real
-- payload.
--
-- The first attempt logged the payload instead. That failed for an operational
-- reason worth recording: `fly logs --no-tail` is hard-capped at 100 lines,
-- which covered a ~19-SECOND window, and streaming through a pipe delivers
-- nothing. A live huddle was run and its payload was gone before it could be
-- read. Persisting the observation removes the race entirely — the row waits.
--
-- CONTROL-PLANE ONLY. This is diagnostic exhaust, not tenant content: it is
-- never read by application code, never fanned out to an ocean, and carries no
-- account_id to isolate on. Registered in `CONTROL_TABLES` in
-- `scripts/check_schema_parity.sh` accordingly, so it is dropped from the
-- control-vs-tenant comparison rather than requiring a paired tenant migration.
--
-- Contents are sanitised BEFORE insert (see `probe_sanitise` in
-- `backend/src/routes/slack_events.rs`): message text and room prose are
-- replaced by character counts, and actor ids are replaced by per-process
-- salted pseudonyms. Channel, team and room ids are kept verbatim because they
-- are routing values we need to read.

CREATE TABLE IF NOT EXISTS slack_huddle_probe (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    observed_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Routing identity, kept verbatim.
    team_id             TEXT NOT NULL DEFAULT '',
    channel_id          TEXT NOT NULL DEFAULT '',
    inner_type          TEXT NOT NULL DEFAULT '',
    subtype             TEXT NOT NULL DEFAULT '',

    -- Which path the `room` object arrived on: 'event.room' when a huddle
    -- message is first posted, 'event.message.room' when `message_changed`
    -- mutates it in place. Settling this is half the point of the probe.
    room_path           TEXT NOT NULL DEFAULT '',

    -- §3.1's two candidate canvas signals.
    canvas_thread_ts    TEXT,
    has_ended           BOOLEAN,

    -- The better candidate, from slack-go's reverse-engineered struct — and
    -- `note_taking` doubles as the §5 "aggregate health surface" signal, i.e.
    -- detecting that Slack's automatic notes are off rather than inferring it
    -- from silence. `transcript` is the one that matters most: a live huddle
    -- produced a transcript with NO notes canvas, which falsifies §3.1's
    -- premise that capture should trigger on the canvas.
    summary_status      TEXT,
    note_taking         BOOLEAN,
    transcript          BOOLEAN,

    -- C12: whether attendance is enumerable. Counts only — the ids themselves
    -- are pseudonymised inside `payload`.
    participants        INTEGER,
    participant_history INTEGER,
    attached_file_ids   INTEGER,

    -- The whole sanitised event, for fields this schema did not anticipate.
    payload             JSONB NOT NULL
);

-- The only access pattern: "show me the most recent observations."
CREATE INDEX IF NOT EXISTS idx_slack_huddle_probe_observed
    ON slack_huddle_probe (observed_at DESC);
