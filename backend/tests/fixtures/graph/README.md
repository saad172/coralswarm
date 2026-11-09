# Microsoft Graph fixtures (Teams + Outlook)

Same register as `backend/tests/fixtures/gmail/README.md`: **hand-authored,
not captured**. This worktree has no live Microsoft 365 tenant / Teams app
registration to probe against (same gap the m365 plan and
`connectors::microsoft`'s own module doc note — Teams channel messages need
`ChannelMessage.Read.All` admin consent, and the transcript/RSC paths need a
provisioned Teams app). Shaped field-for-field against Microsoft's own
documented Graph API schemas — [`chatMessage`](https://learn.microsoft.com/en-us/graph/api/resources/chatmessage),
[`message`](https://learn.microsoft.com/en-us/graph/api/resources/message),
[`subscription` change notifications](https://learn.microsoft.com/en-us/graph/webhooks) —
and against what `backend/src/connectors/microsoft.rs` /
`backend/src/routes/graph_webhook.rs` actually parse, rather than invented ad
hoc, so a future change to `message_to_item`/`teams_message_to_item`/
`email_body::from_graph_body`/the notification envelope parsing is caught by
a fixture diff, not just a hand-rolled `serde_json::json!` literal buried in
a test function. `connectors::microsoft`'s own test module already has a
number of inline-JSON unit tests (`teams_message_maps_html_body_and_namespaces_id`
and neighbors) predating this directory; these fixtures are additive, for the
larger/more-realistic payload shapes and the negative cases below, not a
replacement for those.

All ids/emails/names/tenant ids below are synthetic (`acme-corp.example`,
`aad-user-*`, fictional GUIDs) — there was never real data to redact.
AAD/tenant/subscription GUIDs are made up in the correct shape, not sampled
from a real directory.

## Files

| Fixture | Shape | Exercises |
|---|---|---|
| `teams_channel_message.json` | A single `chatMessage` as `GET .../channels/{id}/messages/{id}` (or a fetched-by-notification-path message) returns it — `messageType: "message"`, a `subject` (channel messages can start a topic), HTML body with an entity. | `teams_message_to_item` (`source_type="teams_channel"`, subject prepended to body, HTML stripped, participant/author captured from `from.user`) |
| `teams_chat_message.json` | Same shape, no `subject` (1:1/group chats never carry one), plain-text body. | `teams_message_to_item` with `source_type="teams_chat"` and no subject prefix |
| `teams_message_bot_authored.json` | `from.application` present, no `from.user` — Graph's shape for a message posted by an application identity (including CoralSwarm's own prompt/confirmation cards). | `teams_message_to_item` returning `None` (humans-only filter, CS-122) |
| `teams_message_system_event.json` | `messageType: "systemEventMessage"` (a membership-change system message), empty body. | `teams_message_to_item` returning `None` for any non-`"message"` `messageType` |
| `teams_message_html_script_style_entities.json` | HTML body containing `<style>`/`<script>` blocks plus `&nbsp;`/`&amp;`/`&mdash;`/`&#39;`/`&lt;`/`&gt;` entities. | `teams_message_to_item` → `body_to_text` → `email_body::from_graph_body` → `html_to_text` — **as of m365 PR 2.0, pins the FIXED behavior**: `<script>`/`<style>` element content is skipped entirely (`.warn{color:red}`/`alert('x');` no longer appear in the shaped output), and `&mdash;` decodes to an em dash. Originally added on #687 to pin the OLD, broken behavior (script/style leakage, `&mdash;` undecoded — CodeRabbit finding F1); m365 PR 2.0 fixed `html_to_text` and flipped this fixture's accompanying test to assert the new behavior instead. See `docs/connectors/teams.md`'s known-gaps section (now marked fixed). |
| `teams_channel_notification.json` | A change-notification `{"value":[...]}` batch for a channel message. `resource` is shaped as the STORED/SUBSCRIBED resource template (`teams_subscription_resource`'s `teams/{team}/channels/{channel}/messages` form, with the message id appended) — the string `is_teams_resource`/`parse_subscribed_teams_resource` actually key on in production (read back from `graph_subscriptions.resource`, never from the notification's own `resource` field for this routing decision — see `process_teams_notification`'s doc comment). **Caveat**: Graph's real per-notification `resource` may render in the OData-canonical parenthetical form (`teams('T')/channels('C')/messages('M')`) instead; unconfirmed without a live capture, and irrelevant to the two functions this fixture exercises either way. | `is_teams_resource` (`/channels/` substring), `parse_subscribed_teams_resource` |
| `teams_transcript_notification.json` | A change-notification batch for a meeting-transcript-available event, `resource` in the `users('u')/onlineMeetings('m')/transcripts('t')` form `fetch_transcript_text`/`meeting_id_from_resource`/`online_meeting_resource_from_transcript` all expect. | `is_teams_resource` (`onlineMeetings` substring), `meeting_id_from_resource`, `online_meeting_resource_from_transcript` |
| `outlook_message_payload.json` | A `GET /me/messages/{id}?$select=...` response: `from`/`toRecipients`/`ccRecipients`, `conversationId`, `categories`, `hasAttachments`, `parentFolderId`, HTML `body` with adjacent `<div>`s and a `<br>`. | `message_to_item` (subject+body join, participants from `from`+`to` only — **not** `cc`, `thread_id` from `conversationId`) and the same `body_to_text` HTML path as Teams, including its "no block-boundary separators" quirk (`<div>A</div><div>B</div>` → `"AB"`, not `"A\nB"` — see `email_body.rs`'s own `html_to_text_still_has_no_block_boundary_separators` pin, still true after m365 PR 2.0). No quoted-reply markers in this body, so `trim_outlook_reply_chain` (new in m365 PR 2.0) is a no-op here — see `microsoft.rs`'s own inline-JSON tests for the trim-wiring cases |
| `outlook_message_html_script_style_entities.json` | The Outlook twin of the Teams script/style/entities fixture above — same `body.content`, different envelope (`GraphMessage` fields). | Proves Outlook and Teams share the exact same `body_to_text` core (`from_graph_body`) and therefore the exact same fix (m365 PR 2.0) — one shared bug, two call sites, both fixtures flipped together so a regression in either is caught independently |
| `calendar_event_teams_invite.json` | A full `event` resource as `calendarView/delta` returns it (no `$select` there — every property, `body` included, HTML): a Teams meeting whose organiser typed a short agenda above the join block Outlook injects (`aka.ms/JoinTeamsMeeting`'s current shape — `me-email-text`, `data-tid="meeting-code"`, the two 80-underscore rules, "Microsoft Teams / Need help?", "Join the meeting now", Meeting ID, Passcode, the inner "Dial in by phone" rule, "Phone conference ID", "For organizers: Meeting options \| Reset dial-in PIN"), plus the `bodyPreview` Graph derives from it (255 chars, `\r\n`, cut mid-passcode). | `shape_graph_event` → `body_to_text_boundaried` → `email_body::trim_teams_meeting_block`: the coral carries the agenda (bullets on their own lines, `&amp;` decoded) and none of the block; the `bodyPreview`-only path (a `$select` without `body`) strips the truncated block to the end and yields the same agenda; and the `$select`ed per-event shape of the same event hashes identically to this full resource |
| `outlook_notification_envelope.json` | A live-subscription mail-message change-notification batch: real subscription id/clientState/resource shape, `resourceData.id` the message to fetch. | `NotificationBatch` parsing; reused directly (not re-typed) by `routes::graph_webhook::tests`' `process_notification_for_a_live_subscription_with_no_credentials_completes_cleanly_pg` pg test, seeding a `graph_subscriptions` row with these exact `subscription_id`/`client_state`/`resource` values so the fixture and the pg assertion can never drift apart |
| `notification_wrong_client_state.json` | Same `subscriptionId` as `outlook_notification_envelope.json`, but a forged/stale `clientState`. | Negative: `client_state_matches` against the REAL secret from the sibling fixture — a realistic-shaped mismatch, not just the short literal strings `client_state_matches_exact_only` already covers in `graph_webhook.rs` |
| `notification_unknown_subscription.json` | A notification for a `subscriptionId` with no backing `graph_subscriptions` row at all — the exact shape Graph would redeliver for a subscription `microsoft_disconnect::purge_microsoft_connection` already purged. | Negative: reused directly by `routes::graph_webhook::tests`' `process_notification_for_a_purged_subscription_is_a_silent_noop_pg` pg test — the fixture-vs-test-DB-state coupling documented above applies here too |
| `malformed_notification_body.json` | Deliberately truncated/invalid JSON — not a parseable `NotificationBatch` at all. | Negative: `routes::graph_webhook::graph_notifications`' `serde_json::from_slice` error arm — the handler must still ACK (`202`) rather than error, so Graph does not retry a batch it can never successfully redeliver |

## Regenerating

Replace with a real capture (pseudonymised, following the `slack_huddle`
recipe: capture raw, redact identity, keep negatives) the first time this
connector's Teams surface is exercised against a live Microsoft 365 test
tenant with the Teams app installed. If you hand-edit
`*_html_script_style_entities.json`'s `body.content`, re-run it through
`connectors::microsoft::body_to_text` (or `email_body::from_graph_body`
directly) to confirm the expected output in the accompanying test — do not
eyeball the entity decoding by hand; `html_to_text`'s named-entity table
(`email_body.rs`'s `NAMED_ENTITIES`) is curated, not exhaustive, and numeric
entities need the decimal-vs-hex/surrogate-range rules in
`decode_one_entity` applied correctly.
