# Google Calendar fixtures

Unlike `backend/tests/fixtures/slack_huddle/` (captured from a real workspace
via a live probe), these are **hand-authored, not captured**: this PR's
worktree has no live Google Cloud project / test Workspace account to probe
against (`workers::google_watch_renewal`'s own module doc notes the same gap
— "not yet exercised against a live GCP project"). They are shaped
field-for-field against Google's own documented `events.list` response
schema (https://developers.google.com/calendar/api/v3/reference/events/list)
rather than invented ad hoc, and pin the specific shapes
`connectors::google_calendar` depends on so a future change to the parsing
code is caught by a fixture diff, not just a hand-rolled `serde_json::json!`
literal buried in a test function. If a live capture becomes possible later,
replace these wholesale and keep this README's "hand-authored" framing
honest about which generation these are.

All ids/emails/names below are synthetic (`acme-corp.example`, fictional
people) — there was never real data to redact.

## Files

| Fixture | Shape | Exercises |
|---|---|---|
| `events_list_page.json` | A full-backfill page (`singleEvents=false`, no `syncToken`): a plain timed event, an all-day event, a recurring master, and a recurring exception (`recurringEventId` set). `nextPageToken` absent, `nextSyncToken` present — this is a TERMINAL page. | `process_events_page`, `shape_calendar_item`, recurring master-vs-exception handling |
| `events_list_incremental_with_cancellation.json` | An incremental (`syncToken`-based) page: one updated event and one `status: "cancelled"` event, `nextSyncToken` present. | `process_events_page`'s cancellation split → `propagate_deletions` |
| `watch_notification_headers.json` | Two header sets Google sends to the `events.watch` channel address: the one-time `X-Goog-Resource-State: sync` bootstrap confirmation, and a real `exists` change notification. No body — Calendar push notifications never carry one. | `routes::google_calendar_push`'s header-driven dispatch (`sync` → ACK only; `exists` → run a sync pass) |

## Regenerating

Replace with a real capture (pseudonymised, following the `slack_huddle`
recipe: capture raw, redact identity, keep negatives) the first time this
connector is exercised against a live Google Workspace test account.
