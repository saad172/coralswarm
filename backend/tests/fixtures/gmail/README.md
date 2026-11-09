# Gmail fixtures

Same register as `backend/tests/fixtures/google_calendar/README.md`: **hand-authored,
not captured**. This worktree has no live Google Cloud project / test Workspace
account to probe against (same gap that connector's README and
`workers::google_watch_renewal`'s module doc both note). Shaped field-for-field
against Google's own documented Gmail API response schemas
(https://developers.google.com/gmail/api/reference/rest/v1/users.messages,
`.../users.messages.history`, `.../users.watch`) rather than invented ad hoc,
so a future change to the parsing/shaping code in `connectors::email_body` is
caught by a fixture diff, not just a hand-rolled `serde_json::json!` literal
buried in a test function.

All ids/emails/names below are synthetic (`acme-corp.example`, fictional
people) — there was never real data to redact. Base64 part bodies were
generated from the plaintext quoted in each file's own note below (not typed
by hand) so they decode to exactly the content the corresponding test
expects.

## Files

| Fixture | Shape | Exercises |
|---|---|---|
| `messages_get_full.json` | A `users.messages.get(format=full)` response for a **reply** message: `multipart/alternative` with a `text/plain` part (a short new reply followed by an `On ... wrote:` quoted chain) and a `text/html` part carrying the same content as `<div>`/`<blockquote>` markup. `From`/`To`/`Cc`/`Subject` headers present. | `email_body::from_gmail_payload` (`text/plain` preference over `text/html`), `trim_quoted_and_signature` (the `On ... wrote:` + `>` quote run), `gmail_message_to_item` (participants from headers, `internalDate` timestamp, `threadId`) |
| `threads_get.json` | A `threads.get(format=full)` response: the two-message thread `messages_get_full.json`'s reply belongs to — a plain-text **original** message (`text/plain`, no multipart, no quoting) followed by the reply. | Multi-message thread shaping (`threads.get` is what `fetch_items`' per-thread opt-in expand will call — G2.3), single-part (non-multipart) payload handling |
| `history_list_page.json` | A `history.list` page mixing `messagesAdded`, `labelsAdded` (an add-on the added message, exercised so a consumer doesn't mistake a label change for a second `messagesAdded`), and `messagesDeleted`, with `nextPageToken` present (non-terminal page). | Shape reference for G2.3's history-page splitting (`process_events_page` analogue) — not consumed by G2.2's own code, shipped ahead per the design doc |
| `users_watch_response.json` | A `users.watch` response: `{historyId, expiration}` (`expiration` epoch-millis-as-string, same convention Calendar's `WatchChannelResponse` uses). | Shape reference for G2.3's `ensure_watch` — not consumed by G2.2's own code |
| `pubsub_push_gmail.json` | A full Pub/Sub push envelope (matches `routes::google_push::PushEnvelope`/`PushMessage`) whose `message.data` is standard-base64 (not base64url — Pub/Sub's own envelope encoding, distinct from Gmail API body parts) of `{"emailAddress":"sam.rivera@acme-corp.example","historyId":9876543}`, `subscription` set to `.../gmail-push-sub` (the discriminator `routes::google_push` matches on per the provisioning script). | Shape reference for G2.3's push dispatch (envelope decode + subscription routing) — not consumed by G2.2's own code |

## Regenerating

Replace with a real capture (pseudonymised, following the `slack_huddle`
recipe: capture raw, redact identity, keep negatives) the first time this
connector is exercised against a live Gmail test account. If you regenerate
`messages_get_full.json`/`threads_get.json`'s base64 body parts by hand,
decode them back through `email_body::from_gmail_payload` to confirm they
still produce the text the accompanying tests assert on — do not eyeball a
base64 string. Also recompute `body.size` as the DECODED byte count (not the
base64 string's length) — CodeRabbit round 1 on PR #681 caught these fixtures
shipping with the base64 length in that field instead.
