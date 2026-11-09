-- Migration: 0277_gmail_hidden_messages
-- Description: Gmail trash/spam tombstones (cs-gmail-trash-org-deletes).
--   One row per coral a REVERSIBLE hide (`labelsAdded TRASH`/`SPAM`)
--   retracted: which message, out of which ocean, for which account. The
--   un-trash / "Not spam" restore re-ingests EXACTLY these rows, back into
--   the SAME ocean, then deletes them.
--
--   ── Why a tombstone, not a coverage rule ──
--   The first cut of the restore path re-ingested a restored message when
--   its thread had ANY `gmail_watched_threads` row or everything mode was
--   on. Both over-reach: a `scope=message` save writes a watched row for the
--   whole thread, so trashing and restoring the conversation ingested every
--   sibling the user never opted into; a `follow=false` thread's new reply
--   that landed in Spam was ingested the moment the user clicked "Not spam".
--   Only what a hide actually took out may come back, and only a record of
--   what was taken out can say what that was.
--
--   ── Why the ocean is on the row ──
--   A hide is graph-confined on BOTH planes (`services::deletion::
--   delete_atoms_in_graph_only`): it retracts the connector's own copy in
--   the ocean the thread was ingested into (or the everything-mode ocean)
--   and never a copy a human promoted elsewhere. The restore must land in
--   that same ocean — re-deriving a destination via `resolve_primary_graph`
--   migrated a thread saved to a non-primary ocean into the primary one.
--   One message can have been retracted from two oceans (saved into one,
--   everything-moded into another), hence `graph_id` in the key.
--
--   A permanent `messagesDeleted` keeps its own semantics (account-wide
--   `delete_source` on the personal plane) and deletes any tombstone for
--   the message. Replayed history is harmless: a repeated hide finds no
--   coral and inserts nothing new (`ON CONFLICT DO NOTHING`), a repeated
--   restore finds no tombstone and falls through to the ordinary
--   live-follow gates.
--
--   Tenant twin: `migrations_tenant/0116_gmail_hidden_messages.sql` —
--   tombstones live wherever the atoms live.
--
-- Depends on: 0186_google_gmail_watch_owner_and_threads (the per-thread
--             opt-in rows the hide resolves its ocean from),
--             0197_gmail_everything_mode (the everything-mode ocean).
-- Down:
--   DROP POLICY IF EXISTS account_isolation ON gmail_hidden_messages;
--   DROP TABLE IF EXISTS gmail_hidden_messages;

CREATE TABLE IF NOT EXISTS gmail_hidden_messages (
    account_id     UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    -- The ocean the corals were retracted from and go back into.
    graph_id       UUID        NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
    -- The mailbox owner whose hide this was — the restore is keyed on
    -- (account_id, owner_user_id, message_id): on the org plane every member
    -- shares one account_id.
    owner_user_id  UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    thread_id      TEXT        NOT NULL,
    -- Gmail's bare message id (`atoms.source_id` without the `:chunk:N`).
    message_id     TEXT        NOT NULL,
    hidden_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (account_id, graph_id, message_id)
);

-- The restore lookup: every ocean this account's message was hidden from.
CREATE INDEX IF NOT EXISTS idx_gmail_hidden_messages_account_message
    ON gmail_hidden_messages (account_id, message_id);

ALTER TABLE gmail_hidden_messages ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON gmail_hidden_messages;
CREATE POLICY account_isolation ON gmail_hidden_messages
    USING (account_id = current_setting('app.account_id')::uuid);
