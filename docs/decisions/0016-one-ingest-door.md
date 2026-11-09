# ADR-0016: One ingest door

**Date:** 2026-10-29
**Status:** Accepted
**Deciders:** Alex
**Builds on:** ADR-0007 (D7 destination invariant), ADR-0015 (retire the desktop app)

---

## Context

Every connector already stores corals through one core, `capture_funnel::store_capture_tx`, and
`tests/capture_funnel_guard.rs` fails the build if anything else inserts atoms, captures source
versions, or reconciles chunks. Extraction is uniform too: one admission check, one `jobs` queue,
one worker.

The front half is not uniform. About 15 paths reach the core two ways:

- **Through the HTTP funnel** (`routes/ingest.rs`): HTTP `/v1/ingest/atom`, MCP `add_context`,
  correction notes, connector "ingest now", Composio, Google, Microsoft, Slack member capture.
  This path runs D7, the content hash, reef anchoring, and the `GenerateMeetingSummary` enqueue.
- **Directly**: Slack events, huddles, Google Chat, GitHub. These write to shared org oceans, so
  they skip D7 and each does its own routing check. They also skip anchoring and meeting
  summaries.

The paths also differ on:

| Concern | Today |
|---|---|
| Durability | Slack events, Graph org: durable job. Graph personal, Slack legacy: inline in the webhook. Gmail push: bare `tokio::spawn`, lost on restart. |
| Embed enqueue | HTTP funnel: `Embed::AfterCommit`, failure only logged. Others: `InTransaction`. |
| R2 upload failure | HTTP, GitHub: `DropRow`. Slack, huddle, Google Chat: `Abort`. |
| Embed dedupe key | Huddle builds its own; everyone else uses `embed_key()`. |

ADR-0015 already removed the desktop-only paths (`ingest_atom_local`, `/sync/push`, the daemon and
staging pipeline) and screen capture with its B4 monitoring-notice gate.

## Decision

### 1. One module, not a service

All ingestion goes through `ingest::submit()` inside the backend. No separate deployable: the
core runs inside a tenant-DB transaction, the `workers` process group already scales on its own,
and a second service would need its own release path.

### 2. Typed destination

Every `IngestRequest` carries a `Destination`:

- `Private { ocean }`: `submit()` applies D7 via `is_own_writable_ocean`.
- `OrgShared { ocean, grant }`: `grant` names the admin-configured routing that allows the write
  (Slack channel mapping, Google Chat space route, GitHub repo binding). `submit()` checks that
  the grant exists and points at `ocean`.

No path reaches the core without a destination decision. There is no "trusted connector" bypass.

### 3. Two stages

- **Receive** (webhooks only): verify the provider signature, write one durable `IngestReceive`
  job holding the raw event or provider id, return 200. No provider calls, no content handling.
- **Submit** (`ingest::submit()`, every path): synchronous, one tenant-DB transaction. Destination
  check, content hash, `store_capture_tx`, embed and extraction jobs enqueued in the same
  transaction. Returns coral ids.

Interactive callers (HTTP, MCP, correction notes, connector "ingest now") call `submit()` directly
and keep the synchronous response with coral ids. Webhook workers call it after fetching full
content. Pollers (Composio, everything-mode, calendar sweeps) call it from their existing workers.
Gmail push and Graph personal gain a few seconds of latency; accepted.

### 4. One rule per concern

- **Embed enqueue:** `InTransaction` everywhere. `Embed::AfterCommit` goes.
- **R2 upload failure:** `Abort` everywhere. Workers retry with backoff; interactive callers get a
  retryable error. `UploadFailure::DropRow` goes.
- **Embed dedupe key:** `embed_key()` everywhere, including huddles.
- **Reef anchoring:** an optional `anchors` field on `IngestRequest`, applied by `submit()`.
- **Meeting summaries:** `submit()` enqueues `GenerateMeetingSummary` for every meeting-kind coral,
  whatever the path. Huddles and Google Meet gain summaries.

### 5. Enforcement

`store_capture_tx` moves into the `ingest` module and becomes private, so a call from outside does
not compile. `capture_funnel_guard.rs` allows `ingest::submit()` as the only entry point; its
`ENTRY_POINTS` list becomes callers of `submit()`.

### 6. Slack legacy webhook is deleted

`/v1/webhooks/slack` (`routes/slack.rs`) duplicates Slack events with inline processing. Delete it
after confirming no installed workspace still posts to it.

### 7. Jev is unchanged

ADR-0013's ingest door was retired on 2026-10-18; Jev's identity work lives in extraction v2,
which every path already shares.

## Delivery

A `gh stack`, one PR per layer, each releasable and revertible:

1. Add `ingest` with `IngestRequest` and `Destination`; move HTTP and MCP onto `submit()` with no
   behaviour change.
2. Move Slack events, huddles, Google Chat and GitHub onto `OrgShared` grants, `Abort`, and
   `embed_key()`; add anchoring and meeting summaries to `submit()`.
3. Add the receive stage; move Gmail push and Graph personal onto it; delete the Slack legacy
   webhook.
4. Make `store_capture_tx` private, tighten the guard, and remove `Embed::AfterCommit` and
   `UploadFailure::DropRow`.

Each behaviour-changing layer adds or updates an e2e scenario (`e2e/README.md`).

## Consequences

- One function creates corals; the compiler enforces it.
- A new connector picks a `Destination` and calls `submit()`. It cannot skip D7 or the grant check.
- No webhook loses work on restart.
- Gmail push and Graph personal corals appear seconds later than today.
- Huddles and Google Meet start producing meeting summaries, which adds LLM spend on those
  sources.
- A failed R2 upload now fails an interactive `add_context` call instead of silently dropping
  the coral.
