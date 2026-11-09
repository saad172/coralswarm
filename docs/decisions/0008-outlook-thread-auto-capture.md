# ADR-0008: Outlook thread auto-capture (whole-conversation ingest + reply subscriptions)

**Date:** 2026-06-19
**Status:** Accepted — implemented (CS-117).
**Deciders:** Alex
**Builds on:** ADR-0006 (the ocean model), CS-116 (per-thread opt-in connector ingest)

---

## Context

The Outlook add-in's "Ingest this thread" button promised a *thread* but delivered a *single
message*: it sent only the currently-open message id, and the Microsoft connector deliberately
fetched "exactly the message the user opted in on — nothing else." Two gaps followed:

1. **Not actually the thread.** Earlier messages in the same conversation were never captured.
2. **Replies were lost.** A new reply arriving after ingest was never captured — the system had no
   way to know about it. The user would have had to re-open and re-ingest manually, and even then
   only the one message.

What was already correct, and stays correct: ingested corals are **permanent**. Atoms are written
with `ON CONFLICT (graph_id, source_type, source_id) DO NOTHING` and never updated or deleted on
re-ingest, so a later reply can never overwrite or remove what is already in the ocean.

The founder asked for both gaps closed: ingest the **whole conversation**, and **auto-capture future
replies**.

## The hard constraint

**Microsoft Graph cannot subscribe to a single conversation.** Change-notification subscriptions are
created on a *mailbox or folder* resource — there is no `conversationId` filter at subscription
creation. The only ways to learn about a reply are to subscribe to the whole mailbox (or a folder)
and filter server-side, or to poll. This constraint, not preference, shapes the design below.

## Decision

**D1 — Ingest expands to the whole conversation.** When the user ingests a message, the connector
fetches it (now including its `conversationId`), then lists every message in that conversation
(`$filter=conversationId eq '…'`, capped at 50 at decision time — since raised to
`MAX_CONVERSATION_MESSAGES = 1000` with an `@odata.nextLink` resume cursor past the cap; m365
Train 2 backfill hardening) and ingests each as a coral. Dedup (`ON CONFLICT DO
NOTHING`) makes re-ingest additive: re-running on a grown thread adds only the new messages and
leaves the originals untouched. The "Ingest this thread" label is now honest.

**D2 — Reply auto-capture via folder-scoped subscriptions per account, filtered to opted-in
threads.** On the first thread ingest for an account we lazily create two Microsoft Graph `created`
subscriptions: one on `me/mailFolders('inbox')/messages` (replies from others) and one on
`me/mailFolders('sentitems')/messages` (the user's own replies, which never land in Inbox), so both
sides of a thread are captured in real time. Graph then notifies our public webhook
(`POST /v1/webhooks/graph`) for **every** new message in those folders — metadata only (message id +
change type), never the body. For each notification we:

1. verify the per-subscription `clientState` secret (constant-time);
2. fetch the message (delegated token, refreshed as needed) to read its `conversationId`;
3. ingest it **only if** that `conversationId` is in `outlook_watched_conversations` for the account
   — otherwise it is discarded without being stored.

So the *subscriptions* are folder-wide (Graph cannot scope to a conversation), but *storage* stays
scoped to threads the user explicitly opted into. We deliberately watch only Inbox + Sent rather than
the whole mailbox (`/me/messages`) to keep the breadth of notifications — and thus the metadata
exposure of D4 — as narrow as the reply-capture goal allows. Each ingested message goes to the ocean
the thread was first ingested into, as the user who opted it in (`ingested_by`), through the
canonical ingest path.

**D3 — Subscriptions are renewed by a background worker.** Graph mail subscriptions live < 7 days
(under 1 day for rich notifications; we use thin). `workers::graph_subscriptions` polls every 30
minutes and `PATCH`es any expiring within 6 hours. A subscription Graph has forgotten, or one whose
account has disconnected, has its row dropped so the next ingest recreates it.

**D5 — Lifecycle notifications + a backstop resync close the recoverability gap.** Microsoft's own
guidance (best-practices doc) is that change notifications can be *missed* — during subscription
lapses, delivery pauses, or downtime — and should be paired with a polling/delta backstop, with
lifecycle notifications handled explicitly. A deep research pass (7 findings, all first-party
Microsoft Learn, verified 3-0) flagged the original notification-only pipeline as the one place the
design diverged from Microsoft's strongest published pattern. So we added:

- A `lifecycleNotificationUrl` (the same `/v1/webhooks/graph` endpoint, branching on the
  `lifecycleEvent` property). `reauthorizationRequired` → extend the subscription;
  `subscriptionRemoved` → recreate it and resync; `missed` → resync.
- A **conversationId-scoped backstop resync** (`workers::graph_subscriptions::resync_account`): every
  6 hours, and on demand for `missed`/`subscriptionRemoved`, re-list each watched conversation
  (`$filter=conversationId eq …`) and re-ingest. Dedup makes it a no-op when nothing changed.

  *Why conversationId-scoped rather than the textbook folder-wide delta query:* a folder
  `…/messages/delta` would surface **all** inbox mail, re-widening the very exposure D2/D4 work to
  contain. Iterating only the opted-in `conversationId`s recovers the same missed replies while
  touching strictly less of the mailbox — tighter than the standard delta approach, and consistent
  with the privacy posture. It does not detect a brand-new thread created entirely during an outage,
  which is correct: there is no opt-in for it yet, so there is nothing to recover.

**D4 — This is a deliberate softening of the CS-116 privacy stance, accepted explicitly.** CS-116
documented that "email is NEVER bulk-pulled… ingested solely via per-thread opt-in." Auto-capture
means the backend now *receives a change-notification* (message id, not content) for all new mail in
a connected account's **Inbox and Sent Items**. It still only ever *fetches and stores* messages
belonging to opted-in threads. The exposure is metadata-on-the-wire for non-opted-in mail; there is
no way to do reply auto-capture on Graph without it. The founder accepted this tradeoff knowingly.

**D6 — Auto-ingest workflows (CS-118).** Users can define rules — *"when a message matches this
filter, ingest its thread"* — so ingestion isn't limited to manual per-thread clicks. A rule's filter
matches on sender address/domain, subject keywords, recipient (to/cc) address/domain, Outlook
category, folder, and has-attachment; semantics are **AND across populated criteria, OR within each**,
and an empty filter is rejected so a rule can never match the whole mailbox. Rules are **per-user**
(owned by a user, ingesting as them into their primary ocean) and scoped to the account's connected
mailbox.

- **Live matching** runs inside the existing webhook: for a notification on a *not-yet-watched*
  conversation, the message is matched against the account's enabled rules; a match watches the
  conversation (so future replies follow via D2) and ingests the whole thread. No new subscription
  machinery — workflows ride the Inbox/Sent subscriptions already in place.
- **Backfill** (chosen explicitly): creating a rule scans the mailbox's most recent messages (bounded
  at 1,000) in the background and ingests existing matches. This is a broad mailbox read — the heavier
  privacy step the founder opted into for backfill — bounded and logged, and it reuses the exact
  in-process matcher so backfill and live matching agree.
- **Privacy delta:** a workflow widens *storage* from "threads I clicked" to "threads matching my
  rules." That is the user's own rule, applied to their own mailbox, and still scoped — non-matching
  mail is never stored. Disabling/deleting a rule stops future matches but leaves already-ingested
  corals and watched conversations intact.

## Consequences

- **Add-in unchanged.** It still sends one message id; the expansion + watch registration happen
  server-side. No add-in redeploy is required for the behaviour change.
- **New schema (0035):** `outlook_watched_conversations` (which threads to capture, the destination
  ocean, the opting-in user) and `graph_subscriptions` (the live subscription + its `clientState`
  secret + expiry). **(0036):** `outlook_workflows` (per-user auto-ingest rules — D6).
- **Ops prerequisites:** the webhook `notificationUrl` must be public HTTPS and echo Graph's
  validation token within 10s. Prod (`coralswarm-backend.example`) satisfies this; **local testing
  needs a tunnel** (subscription creation fails otherwise, and since `after_ingest` is best-effort the
  ingest still succeeds — just without auto-capture). No new OAuth scope: delegated `Mail.Read`
  already covers inbox subscriptions, and `offline_access` already provides the refresh token used for
  background fetch + renewal.
- **Bounded fan-out:** conversation expansion is capped (50 messages at decision time; now
  `MAX_CONVERSATION_MESSAGES = 1000` with a persisted resume cursor past the cap); the existing
  per-request item cap on the ingest endpoint is unchanged.
- **Failure isolation:** watch registration + subscription creation run as a best-effort
  `after_ingest` side effect — if Graph or the DB hiccups, the user's ingest still succeeds; only
  auto-capture is deferred until the next ingest or worker pass.
