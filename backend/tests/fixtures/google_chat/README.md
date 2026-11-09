# Google Chat fixtures

Like `backend/tests/fixtures/google_calendar/` and unlike
`backend/tests/fixtures/slack_huddle/` (captured from a real workspace via a
live probe): **hand-authored, not captured.** This PR (gsuite Train G4 PR
2 — `.scratch/gsuite-enterprise/g4-chat-design.md` §7 "G4.2") lands in
parallel with G4.1 and before the design's step 0 probe against the dogfood
Workspace tenant, so there is no live capture to draw from yet.

Shaped field-for-field against Google's own documented resource schemas:

- `Space` / `spaces.list` / `spaces.get` —
  https://developers.google.com/workspace/chat/api/reference/rest/v1/spaces
- `Message` / `spaces.messages.list` / `.get` —
  https://developers.google.com/workspace/chat/api/reference/rest/v1/spaces.messages
- `Membership` / `spaces.members.list` —
  https://developers.google.com/workspace/chat/api/reference/rest/v1/spaces.members
- `Subscription` (Workspace Events API) —
  https://developers.google.com/workspace/events/guides/events-overview
- Chat app interaction events (`ADDED_TO_SPACE` etc.) —
  https://developers.google.com/workspace/chat/api/guides/events

**Two files are marked UNVERIFIED at the field-shape level, per the design
doc (§2.7, §6):**

- `space_events_list.json` — the exact `SpaceEvent` payload shape per event
  type is not enumerated in Google's method reference (only the request
  filter contract is documented). The event envelope (`name`, `eventTime`,
  `eventType`) and the top-level `messageCreatedEventData`/
  `messageDeletedEventData`/batch wrapper are a best-effort construction
  from the general Workspace Events shape, not a captured example.
- `pubsub_push_chat_message_created.json` — the design doc (§1.4, §2.6)
  notes that whether the Workspace Events CloudEvent envelope's `message.data`
  is itself a full `SpaceEvent` JSON body (as constructed here, now trimmed
  to a bare `{name}` message per D5 below) or something narrower is
  unconfirmed pending step 0's real capture. The Pub/Sub push envelope shape
  itself (`message.{data,messageId,publishTime,attributes}` + top-level
  `subscription`) is the platform-standard Pub/Sub push format and is not in
  question; only the base64-decoded `data` payload's exact internal shape
  is. The binary-mode CloudEvent `ce-*` attributes (`ce-id` = the
  `SpaceEvent` resource name, `ce-source` = the subscription's resource
  name, `ce-subject` = the affected space) follow the platform-documented
  CloudEvents-over-Pub/Sub contract and are **not** in the UNVERIFIED
  category — CodeRabbit finding on #707, fixed. So is the `data` payload's
  content itself: the design's own D5 decision (§4.2 — "no resource data in
  the payload" for a subscription that omits `payloadOptions`) means the
  embedded message must carry only its resource `name`, never `sender`/
  `text`/`formattedText` — the fixture used to embed the full message body,
  which actively contradicted D5 rather than merely being unconfirmed;
  fixed to a bare `{name}` for the same reason. Only the *envelope* shape
  around that bare name (whether it's really a full `SpaceEvent` wrapper,
  a narrower `messageCreatedEventData`-only body, or something else) stays
  UNVERIFIED pending step 0's capture.

Both should be **replaced wholesale** with a real capture the first time
step 0 (or any later PR) runs against a live Chat app + Workspace Events
subscription — same "regenerate, don't patch" posture the calendar
fixtures' README documents.

All ids/names/emails below are synthetic (`spaces/AAAAAAAAAAA`, fictional
people like "Alice Ng"/"Bob Chen") — there was never real data to redact.

## Files

| Fixture | Shape | Exercises |
|---|---|---|
| `spaces_list.json` | Five spaces covering every archetype: one `SPACE` (`HISTORY_ON`), one `SPACE` (`HISTORY_OFF`), one `GROUP_CHAT`, one `DIRECT_MESSAGE`, one `DIRECT_MESSAGE` with `singleUserBotDm: true` (our own bot's DM — never ingestible). `nextPageToken` present (non-terminal page). | `Archetype::of` over all five shapes, incl. the negative-sweep guard that nothing but `spaceType` decides it |
| `space_get.json` | A single `SPACE` with `spaceThreadingState`, `historyState`, `membershipCount` populated. | Space-detail shaping (G4.3+) |
| `members_list.json` | Seven memberships: five `JOINED` (four `HUMAN`, one `BOT`), one `INVITED` `HUMAN` (not yet accepted — excluded from seating), one `groupMember` (a Google Group — inexpansible without a Directory scope, §3.4a). Two of the four `JOINED` humans (`users/1001` Alice Ng, `users/1002` Bob Chen) are the ones a future G4.4 org fixture is expected to map to CoralSwarm accounts; the other two (`users/1003`, `users/1004`) are intentionally left unmapped. | Roster auto-seating counting rules (G4.4) — `JOINED`-only, bot-excluded, group-counted-separately |
| `messages_list_page1.json` / `_page2.json` | A full backfill page pair: interleaved senders across both pages, a threaded reply (`thread.name` + `threadReply: true` on `msg-0003`), a `formattedText` user mention (`<users/1001>` on `msg-0002`, with a matching `annotations[]` entry whose `startIndex`/`length` exactly spans `"@Alice Ng"` in `text`), a `cardsV2` bot message (`msg-0004`), and an attachment (`msg-0005`). Page 1 carries `nextPageToken`; page 2 has none (terminal page). | `chat_message_to_item`'s eight rules end-to-end; pagination shaping (G4.4) |
| `message_get.json` | `msg-0002` re-fetched with `lastUpdateTime` now set — the edited-message case. | §5.3 / conformance test 8′: an edit is reported upstream but not honoured in v1 |
| `space_events_list.json` | One `message.v1.created` event, one `message.v1.deleted` event (`messageDeletedEventData.message` carrying the documented `name`/`createTime`/`deletionMetadata` triple, not `deleteTime` — Google's own reference for `MessageDeletedEventData` populates only those three), and one `message.v1.batchCreated` event wrapping two messages, in a single page. **Envelope shape UNVERIFIED — see above.** | `spaces.spaceEvents.list` catch-up decode (G4.4) |
| `pubsub_push_chat_message_created.json` | A full Pub/Sub push envelope: `message.data` base64-encodes an `event-created-0001`-shaped body whose nested message carries only `name` (no `sender`/`text`/`formattedText` — D5, §4.2: this subscription omits `payloadOptions`, so no resource data ships in the payload), plus binary-mode CloudEvent `ce-*` attributes (`ce-id` = the `SpaceEvent` resource name, `ce-source` = the subscription's resource name, `ce-subject` = the affected space — the platform's documented CloudEvents-over-Pub/Sub attribute contract). `subscription` ends `chat-events-sub`. **Envelope shape UNVERIFIED — see above.** | `routes::google_push`'s `classify_subscription` → `PushSurface::Chat` dispatch (G4.4) |
| `pubsub_push_chat_app_added_to_space.json` | A Chat app interaction event (`ADDED_TO_SPACE`, base64-encoded in `message.data`, no `ce-*` attributes — this delivery channel is Chat's own interaction-event JSON, not a Workspace Events CloudEvent) whose `subscription` ends `chat-app-events-sub`. | `routes::google_push`'s second, distinct subscription arm (G4.3) — must not be conflated with the Workspace Events decoder |
| `subscriptions_create_response.json` | A completed `google.longrunning.Operation` (`done: true`) wrapping the created `Subscription` in `response` — `subscriptions.create`'s actual documented response shape, not a bare `Subscription` (same `Operation`-not-`Subscription` contract `subscriptions_patch_operation.json` documents for the renewal PATCH, §1.5): `targetResource: "//chat.googleapis.com/spaces/-"`, the four subscribed event types, `expireTime` ~7 days out (no `payloadOptions` ⇒ the D5 7-day TTL, §4.2). | `ensure_subscription` (G4.4) |
| `subscriptions_patch_operation.json` | A `google.longrunning.Operation` — **not** a bare `Subscription` — wrapping the renewed subscription in `response`, `done: true`. Mirrors the shape `workers::google_watch_renewal::renew_chat` already parses via `expire_time_from_operation` (§1.5, #674 round-1 finding 4). | `renew_chat`'s `Operation`-not-`Subscription` parsing (already merged; this fixture documents the contract, no new consumer in this PR) |

## Regenerating

Replace with a real capture (pseudonymised, following the `slack_huddle`
recipe: capture raw, redact identity, keep negatives) once step 0's probe —
or any later PR — runs against a live Chat app + Workspace Events
subscription in the dogfood Workspace tenant.
