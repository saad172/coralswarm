# Slack bot-message fixtures — SYNTHETIC

**Every value in these files is invented.** No workspace, channel, user, bot,
app, repository, email address, URL, figure or message body here is real. Ids
are obviously fake (`T0TESTWS0`, `B0TEST…`, `U0TEST…`, `A0TEST…`), links point
at `example.com`, and the one email address is `someone@example.com`.

What *is* real is the **shape**. Each file reproduces the structure of a
message object observed read-only via `conversations.history` on 2026-09-09:
the same keys in the same order, the same nesting, the same presence and
absence of fields, and content of roughly the same length. That structure is
the entire reason these fixtures exist — the flattener's behaviour turns on
which fields are populated, not on what they say.

Do not paste production payloads in here. If a new shape needs covering, add a
synthetic file for it and record what it is modelled on.

## A. `conversations.history` message object vs `event_callback` `event` object

Both are "a message", but they are NOT interchangeable. Wrap a fixture like
this before feeding it to the webhook handler (`routes::slack_events`); the
test helper `slack_event_envelope` does exactly this:

```json
{ "type": "event_callback", "team_id": "T0TESTWS0",
  "event": { <fixture>, "type": "message", "channel": "<C…>", "channel_type": "channel",
             "event_ts": "<same as ts>" } }
```

| field | history | event | note |
|---|---|---|---|
| `channel` | **absent** | present | you asked which channel; the event tells you. The handler reads `event["channel"]`. |
| `team_id` | absent | **envelope only**, not inside `event` | the handler reads `payload["team_id"]`, not `event`. |
| `event_ts` | absent | present | delivery time; `ts` remains the message time. |
| `channel_type` | absent | present (`channel`/`group`/`im`) | not read by the handler today. |
| `type` | `"message"` | `"message"` | same. |
| `ts`,`text`,`user`,`bot_id`,`subtype`,`thread_ts`,`blocks`,`attachments`,`app_id`,`bot_profile` | present | present | identical shape — this is why the fixtures are reusable. |
| `root` (on `thread_broadcast`) | present (full nested parent message) | present per Slack docs — **NOT verified**, events cannot be replayed | treat as unconfirmed. |
| `edited`, `client_msg_id`, `team`, `metadata` | present | generally present | incidental. |

Fields the handler actually reads: `payload.team_id`; `event.type`, `.subtype`,
`.bot_id`, `.text`, `.channel`, `.user`, `.ts`, `.thread_ts`.

## B. Where the human-readable content lives, per fixture

| file | modelled on | text | content actually lives in |
|---|---|---|---|
| `github_pr_opened_attachments.json` | a GitHub app "PR opened" notice | **""** | `attachments[0].pretext` + `.title` + `.text` (+ `.fields[]` = Comments count, `.footer` = repo) |
| `github_commit_attachments.json` | a GitHub app push notice | **""** | `attachments[0].pretext` (who/branch/compare link) + `.text` (one line per commit). No `title`. |
| `github_thread_broadcast.json` | a GitHub app "PR merged" broadcast, `subtype:"thread_broadcast"`, `thread_ts` set | **""** | `attachments[0].pretext` + `.title`. **No `.text`** — title is the whole payload. Also carries `root` (the full parent message, with its own 8 KiB body). |
| `app_rich_text_blocks.json` | an alerting app posting markdown | **populated** | `text` AND `blocks[0]` (`rich_text` → `elements[].elements[]` w/ `text`/`emoji`/`link` nodes). `text` is the richer rendering and wins. |
| `claude_app_message.json` | an assistant app's install notice | populated but **truncated to the first sentence** | `blocks[0..1].text.text` (`mrkdwn`) + `blocks[2].elements[].text` (`context`). `text` alone loses ~80%, so the blocks win. |
| `own_bot_confirmation.json` | **our own** app's backfill confirmation (`B0TESTBOT0` / `A0TESTAPP0`) | populated | `text` (and an equivalent `rich_text` block). Self-authored notice — **must be excluded from ingest**. |
| `bot_message_subtype.json` | a legacy `subtype:"bot_message"` post | populated | `text` + `rich_text` block. **No `user` field, no `bot_profile`** — identity is `username` only. Any code doing `event["user"]` gets `"unknown"` here. |
| `human_plain.json` | a person typing in a channel | populated | `text` (+ `rich_text` block with a `user` mention node). Contrast case. |

## C. Is `attachments[i].fallback` alone adequate for a GitHub-shaped post? **No.**

`fallback` is a one-line notification summary; it drops the PR/issue title and
the entire body. The fixtures preserve the measured ratio:

| fixture | `fallback` | `pretext`+`title`+`text` | fallback retains |
|---|---|---|---|
| PR opened | 58 chars | **8234 chars** | **0.7%** |
| commit push | 58 chars | ~440 chars | ~13% |
| thread_broadcast (PR merged) | 58 chars | ~250 chars | ~23% |

The PR-opened `fallback` does not even contain the PR number or title. **Flatten
`pretext` + `title` + `text` (+ `fields[].title: fields[].value`), and use
`fallback` only as a last resort when all of those are empty.** Note
`mrkdwn_in: ["text"]` — `text`/`pretext`/`title` carry Slack link syntax
`<url|label>` and backtick code spans, which this connector deliberately stores
verbatim (see the module doc on why it applies no mrkdwn normalisation).

## D. Other flattening gotchas these fixtures pin

- `blocks` come in two shapes: `section`/`context` (content at
  `blocks[i].text.text` / `blocks[i].elements[j].text`, mrkdwn) and `rich_text`
  (content nested at `blocks[i].elements[j].elements[k]` as
  `text`/`emoji`/`link`/`user` nodes). A flattener must handle both; only
  `rich_text` requires recursion.
- **92% of bot messages in the workspace this was measured against (235 of 253
  since 2026-08-01) have an empty `text`.** Removing a `bot_id` filter alone
  therefore changes nothing — the next guard drops empty `text`.
- **65% carry `thread_ts` and a third are `thread_broadcast`**, which re-posts
  an already-existing threaded message to the channel and carries the full
  parent under `root` — never flatten `root`, or every merged PR is ingested
  twice. `github_thread_broadcast.json` exists to pin exactly that.
- Our own app (`B0TESTBOT0` / `A0TESTAPP0` here) must stay excluded, or the
  connector's own "Ingested N past messages" confirmations feed back into the
  ocean.
