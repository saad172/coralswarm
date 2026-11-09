# ADR-0007: Web v1 — Tenancy, Encryption, RAG & Inference

**Date:** 2026-06-15
**Status:** Accepted — **design only, not yet implemented.** Supersedes the earlier "active ocean" /
"Managed-default" thinking. Implementation tracked in `HANDOFF_WEB_V1.md` (build sequence).
**Deciders:** Alex
**Builds on:** ADR-0004 (local-first / server-trust), ADR-0005 (local store, sync, encryption),
ADR-0006 (the ocean model + promotion)

> **Amended by [ADR-0009](0009-conversational-ask.md) (2026-06-19):** §D9's
> "server-blind private ocean" guarantee is **narrowed** — conversation
> transcripts (Ask chat questions + answers) are stored server-side for all
> ocean tiers, including the private Primary ocean. Retrieval over the private
> ocean remains in-browser and server-blind; only the transcript is now
> server-visible. See ADR-0009 for the rationale and a path to restore E2E.

---

## Context

CoralSwarm's web app (`web/`, Next.js 15) must support and **publicly announce three tiers —
individuals, teams, and organizations** — for v1. A multi-agent gap audit across backend API / web
client / web screens / product docs / auth found that the tenancy machinery is *structurally present
but not delivered to readers*: shared oceans never reach the people they were promoted to, two flows
are broken or fake, and several backend capabilities have no web surface.

ADR-0006 defined *what oceans are* and *how knowledge moves between them* (promotion). This ADR fixes
the layer above it for web v1: **who can see which ocean (tenancy), how each tier's keys are
custodied (encryption & recovery), how the ask loop reads across oceans (RAG), and where inference
runs (cloud vs local).** It locks the design before the build so the backend, web client, auth, and
crypto all share one mental model.

The throughline: **shared (team/org) oceans are social and server-backed; the private ocean is a
local, end-to-end-encrypted mode; promotion is the deliberate, consent-gated bridge between them.**

Product vocabulary (the ocean lexicon): **coral = atom**, **reef = node**, **ocean = graph/scope**,
**currents = edges/activity**.

## Decision

Decisions are labelled **D1–D17** to match `HANDOFF_WEB_V1.md` and the gap audit.

### Scope

- **D1 — v1 announces individuals + teams + orgs.** Tenancy sharing (cross-member reads, account
  switch, team lifecycle) must *actually work*, not be hidden. Genuinely-fake placeholder UI is still
  stripped. *Why: the positioning announces team/org support, so ADR-0006's "promote → others can
  read" promise must deliver.*

### Tenancy model

- **D2 — org = account = a Clerk Organization.** A solo individual is an org-of-one. A user in
  multiple orgs = multiple accounts → account switch. *Why: maps cleanly onto the existing accounts /
  `team_members` / envelope-key tables and ADR-0006.*

  **D2-impl (resolved 2026-06-15) — two account kinds, not one.** The schema has no per-user owner on
  `graphs`, so a single shared account cannot tell whose private ocean is whose. We therefore keep
  **two account kinds:**
  - a **personal account-of-one** per user (the existing `auth.rs:370` auto-provision stays) — holds
    that user's **private Primary ocean** (`kind='personal'`, `is_primary`), client-side E2E,
    server-blind. **One per user, shared across all their orgs** (the private ocean is the user's, it
    does not fragment per employer).
  - an **org account** per Clerk Organization, provisioned by webhook (D3) — holds the **org ocean**
    (`kind='org'`) and the **team oceans** (`kind='team'`, via `teams`/`team_members`). Users are
    `account_members` of it.

  The org account is identified by a new `accounts.clerk_org_id` (NULL ⇒ personal account).
  **Effective read scope for a user** = their personal account ∪ each org account they belong to
  (org ocean) ∪ the team oceans where they are an active `team_member`. This is the union D9 reads
  over, and the fix for gap 2.
- **D3 — Clerk Organizations are the source of truth** for org signup, member invites, roles, SSO,
  and domains. The backend provisions account + membership + keys via Clerk `organization` /
  `organizationMembership` **webhooks**. *Why: reuses Clerk's org/invite/role/SSO instead of
  reinventing; replaces the broken hand-rolled `teams.invite` (gap 1). Today accounts auto-provision
  per-user (`backend/src/auth.rs:370`) with no org concept — that changes to Clerk-driven
  provisioning.*
- **D4 — active scope comes from the Clerk JWT.** `require_auth` reads `org_id` (and `org_role` for
  authz) from the already-verified Clerk session token; no active org → personal account. No custom
  `X-Account-Id` header. The web account switcher is Clerk's `<OrganizationSwitcher>`. *Why:
  tamper-proof (Clerk-signed), nothing extra for the client to send, authz for free. Retires the
  "future header" TODO at `backend/src/middleware/auth.rs:99`.*

### Encryption & B2B recovery

- **D5 — per-tier key custody.** Team/org oceans are **server-custodied** (the server can read them →
  cloud synthesis works). The private Primary ocean is **client-side end-to-end encrypted, server
  blind.** *Why: the cloud ask loop must read shared content to synthesize; private stays blind to
  the operator/server.*
- **D6 — B2B recovery = M-of-N Shamir org escrow.** Each user's private-ocean key is wrapped to
  (their own credential) **and** an **org recovery key**. The org recovery key is **Shamir-split
  across N admins, M required** to reconstruct — all client-side. The server stores only opaque
  wrapped blobs (stays blind). No single admin can unilaterally decrypt. **Disclose to employees:**
  "private" means private from coworkers, from shared synthesis, and from the server-at-rest — but
  **org-recoverable by M admins.** Every recovery is logged. *Why: B2B needs continuity (departures,
  lost passphrase, legal hold) without giving the server a backdoor.*

### Ocean / RAG / inference model

*(This section supersedes the earlier "active ocean" + "Managed-default" thinking.)*

- **D7 — ingest is always personal.** Every captured coral lands in the user's **private ocean**;
  there is no destination picker. *(Keeping `corals.ingest` at `scope:'private'` is correct — gap
  5's "ingest side" was never a bug.)*
- **D8 — promotion is the only path to shared oceans**, at two granularities: promote a **reef**, or
  promote the **source instance** that generated the corals (a meeting / huddle / thread → all its
  corals together). One-way, widen-only, **copy** (ADR-0006). It is the single point where private
  content crosses the E2E boundary into server-readable storage → **gate it with explicit consent +
  clear wording** at that moment. **Source-level promotion is net-new backend work** (the audit found
  only reef-level promote, now landed in ADR-0006).
- **D9 — RAG reads across ALL oceans the user can access** (private + their teams + org) by default;
  the user can narrow via a scope **filter**. The ocean switcher is a **filter, not a mode
  selector.** *Why: only the device can span private (E2E) + shared data, which is what makes
  local-first natural.*
- **D10 — inference is a user preference, on the RAG/answer side only.** **Web defaults to cloud
  (ZDR) synthesis**; **desktop is the full local-inference home** (ADR-0004). On web: **local
  retrieval always** (in-browser ONNX embeddings + a vector index over *decrypted* private corals in
  encrypted IndexedDB — still required because the server is blind to private), **synthesis defaults
  to cloud under ZDR**, and local synthesis is opt-in for users who run Ollama/WebLLM. *Why: a real
  local LLM in a browser tab is a multi-GB download or a localhost-Ollama dependency — too heavy as
  the web default; local retrieval (a small ONNX model) is fine.*
- **D11 — cloud opt-in = consented egress of the full retrieved union.** When cloud synthesis is on,
  the assembled context (private + shared plaintext) goes to Anthropic, protected by
  **no-training-by-default + ZDR**, with explicit disclosure. An org egress policy (in Clerk org
  metadata) can pin or forbid modes. **ZDR orgs must NOT route to Claude Fable 5** (Fable 5 requires
  30-day retention → 400s under ZDR; use Opus / Sonnet / Haiku).

### Meetings & connectors on web

- **D12 — meetings enter web as transcripts** (paste/upload now; a Zoom/Meet/Teams transcript
  connector later). Live on-device capture stays a **desktop-only** feature. Replace the "Capture
  meeting (on-device Whisper)" web tile with **"Add meeting transcript."** Meetings remain a
  first-class promotable source on web (supports D8).

### Curation & cleanup

- **D13 — all three curation surfaces ship in v1:** reef rename/retype (`PATCH /v1/nodes/{id}` —
  gap 6), candidate corals (`GET /v1/nodes/{id}/candidates` — gap 7), and supersession edges
  (gap 8). The backend for all three already exists; only the web client is missing.
- **D14 — ⌘K: build it** into a global "ask" command palette over the union (scope-filterable),
  rather than stripping it (gap 12).
- **D15 — hide Gmail/Zoom "coming soon" tiles on web v1** (gap 11); keep Slack (real) + the new
  transcript ingest.
- **D16 — surface citations prominently** (the product's "cited answer" promise); tuck **token cost**
  behind a usage/detail view (gap 10).
- **D17 — gap 1 (broken `teams.invite` 422) is moot** — invites now go through Clerk Organizations
  (D3).

## Consequences

- **Backend.** Per-user auto-provisioning (`auth.rs:370`) is replaced by Clerk-webhook-driven
  account + membership provisioning. `require_auth` gains JWT `org_id`/`org_role` parsing.
  `list_graphs` (`store/libsql.rs:617`) and retrieval scope (`routes/queries.rs:225`) must join
  `team_members` so shared oceans actually reach readers (gap 2 — the headline fix). Source-level
  promotion (D8) and the `account_prefs` / org-egress-policy surfaces (D11, gap 4) are net-new.
- **Web.** Adds `<OrganizationSwitcher>`, an in-browser retrieval engine (`onnxruntime-web` +
  encrypted IndexedDB, reusing `packages/embeddings/`), union-RAG assembly, synthesis routing with
  disclosure, the ocean scope filter, curation surfaces, the ⌘K palette, prominent citations, and
  the transcript-ingest tile. Strips the fake connector/LLM toggles and Gmail/Zoom tiles.
- **Crypto.** Two custody regimes coexist (server-custodied shared DEKs; browser-E2E private keys)
  plus the M-of-N Shamir org-escrow ceremony and recovery audit log.
- **Trust posture.** The server stays blind to private oceans at rest; cloud synthesis is an
  explicit, disclosed, per-org-policy-gated egress of the retrieved union.
- **Human prerequisites (not code):** (a) enable Organizations in the Clerk dashboard + configure the
  webhook endpoint/signing secret; (b) request **ZDR** from Anthropic Sales for the org tier and
  verify in Console → Settings → Privacy Controls.

## Grounding facts (Anthropic data policy — verified 2026-06-15)

- By default Anthropic **does not train** on commercial/API inputs or outputs (only via explicit
  thumbs-up/down feedback or affirmative consent — so wire up **no** feedback-submission path).
- **ZDR** is available for eligible APIs via Anthropic **Sales** (per-org approval); verify in
  Console → Settings → Privacy Controls → Data retention period. Even under ZDR, only **User Safety
  classifier results** are retained, not content.
- **Fable 5 is not available under ZDR.**
- Sources: privacy.claude.com articles 7996868 (training) and 8956058 (ZDR coverage).

## References

- ADR-0004 (local-first), ADR-0005 (local store/sync/encryption), ADR-0006 (ocean model + promotion).
- `HANDOFF_WEB_V1.md` — the dependency-ordered build sequence (Phase 0–4) implementing D1–D17.
- Gap-audit workflow result: 12 grounded gaps.
