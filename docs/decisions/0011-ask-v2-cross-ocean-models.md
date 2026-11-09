# ADR-0011: Ask v2 — cross-ocean query, model selection, navigation integration, robustness

- **Status:** Accepted
- **Date:** 2026-06-26
- **Amends:** ADR-0009 §2 (a conversation is scoped to exactly one ocean) and
  ADR-0009 §9 (share read-only) — see "Amendments to ADR-0009".
- **Builds on:** ADR-0006 (ocean model), ADR-0007 (web tenancy / RAG / inference,
  esp. D5 key custody + D10 web inference), ADR-0009 (conversational ask),
  ADR-0010 (access + GTM model).
- **Surface:** Web only. Desktop/offline is out of scope for this ADR.

## Context

The Ask page (`web/app/(app)/ask/page.tsx`, backend
`POST /v1/queries/stream` in `backend/src/routes/queries.rs`) is a persisted
chat hard-scoped to a **single** ocean (`graph_id`), chosen in the global
sidebar (`Shell.tsx`). The synthesis model is **instance-wide**
(`SYNTHESIS_MODEL` env, default `claude-haiku-4-5`); there is no per-request
model choice. Switching oceans resets the thread and the conversation list.

We want Ask to: (1) query **one ocean or several oceans at once**, (2) let the
user **choose the model**, (3) be **more integrated with navigation**, and (4)
be **more robust** — with a specific requirement that cross-ocean answers
**cite which ocean each claim came from** and **surface contradictions** between
oceans.

### Architectural constraint that shapes everything

Per ADR-0007 D5/D10, the private **Primary** ocean is **server-blind**:
retrieval over it runs **in-browser** (`web/lib/engine/privateEngine.ts`) and is
attached to a turn as separate `localHits`. The server-side retrieval +
synthesis pipeline can only read **server-custodied** (team/org) oceans. So
"query cross-ocean" can only merge server-custodied oceans server-side; the
private ocean is a different retrieval path whose hits can never be fed to cloud
synthesis. (In practice the private ocean does have a server `graph_id` with
server-stored atoms that *do* synthesize; only the browser-only `privateEngine`
atoms stay on-device. This ADR keeps the private path as its own mode regardless.)

## Decisions

### A. Scope & cross-ocean

1. **Scope picker on the ask page** (not the global sidebar): **multi-select**
   over the user's reachable **server-custodied** oceans, plus an "All" shortcut.
2. Ask-page scope state is **independent** of the global sidebar ocean, but
   **seeded** from it on first load. (The global selector still drives Reefs,
   Currents, Review, etc.)
3. The private Primary ocean is a **separate, mutually-exclusive mode** —
   "Ask Private · on-device" vs. "Ask shared oceans (multi-select)". Private is
   never mixed into a multi-ocean shared query, so an answer never carries
   dangling, uncited on-device hits.
4. **Retrieval:** fan out per-ocean retrieval **in parallel at full candidate K**
   over the selected oceans (reuse the existing per-`graph_id` path), merge into
   one candidate pool, **rerank globally by relevance** (no fixed per-ocean
   quota).
5. **Per-ocean floor guarantee:** before capping the synthesis context, every
   in-scope ocean that has any hit clearing a relevance floor contributes **at
   least its top source** to the set handed to contradiction detection +
   synthesis. Without this, a minority ocean holding the contradicting fact could
   be reranked out before detection ever sees it.
6. **Synthesis-context cap scales with ocean count up to a ceiling** (so the
   prompt grows with N but stays bounded).

### B. Per-ocean citation & contradiction surfacing

7. Every citation chip / source row carries a **per-ocean badge** (ocean name +
   hue). Each retrieved source already knows its `graph_id`.
8. **Structured contradiction detection (approach B):** a separate structured
   LLM pass over the merged sources emits
   `[{ topic, positions: [{ ocean_id, ocean_name, citation_index, claim }] }]`,
   rendered as a **distinct "⚠ Sources disagree" card** (not just narrated prose).
9. The detection pass is **sequential** — it runs **before** synthesis and its
   output is injected into the synthesis prompt, so the prose itself is
   contradiction-aware (no confident prose contradicting the card).
10. Detection is **gated to 2+ oceans** and is **cross-ocean only** — single-ocean
    asks skip it entirely (zero added latency in the common case). It always runs
    on a **Haiku-class** model regardless of the user's synthesis-model choice.

### C. Conversations

11. A conversation carries a **scope descriptor** `{ mode: shared | private,
    ocean_ids: [...] }` instead of a single `graph_id`. Each **turn** snapshots
    the scope actually used (history stays truthful when scope changes mid-thread).
12. The conversation sidebar shows **all** of the user's conversations regardless
    of the global active ocean, each tagged with ocean badge(s). (This is also
    part of "more integrated with navigation".)
13. Reopening a conversation **restores its scope** into the picker. New turns
    default to the conversation's current scope; changing it updates the
    conversation's current scope going forward.
14. A conversation is **mode-locked** (private vs shared) for its lifetime.

### D. Model selection

15. Expose **Fast / Smart / Max** → `claude-haiku-4-5` / `claude-sonnet-4-6` /
    `claude-opus-4-8`, using **capability labels** (raw model ID + "best for…"
    shown on hover); labels survive version bumps.
16. New `model` field on the stream request, **validated server-side against an
    allowlist** (never pass a raw client string to the Anthropic API).
17. Selector lives on the **composer** (model chip next to send), **sticky to the
    conversation**, with a **user-level default** in Settings.
18. Auxiliary passes (contradiction detection, extraction) always run on Haiku —
    the user's model choice governs the **answer prose** only.
19. **BYOK is out of scope** for v1.

### E. Navigation integration

20. **Click-through citations:** a citation chip / source row opens the source
    coral/reef in a **side peek/drawer** (don't lose the conversation).
21. **Contextual "Ask about this":** from a reef/ocean/coral page, an entry point
    that deep-links into Ask with **scope pre-seeded** to that entity. It seeds
    **scope only** — no auto-generated starter question.
22. **URL-as-state:** Ask reads/writes `scope` (ocean set + mode), `model`, and
    `conversation` in the URL — deep-linkable, shareable link, back/forward works.

### F. More options (per-ask controls)

23. Behind an **"Options" disclosure** (scope + model stay inline on the composer):
    source/connector filter (`source_type`, already in the backend), a
    **Fast ↔ Thorough depth toggle** (maps to `top_k` + context size), **answer
    style** (concise / detailed / sources-only), **regenerate**, and
    **copy/export** (answer + citations).

### G. Robustness

24. **Empty/thin-retrieval honesty guard:** when the merged candidate set is empty
    or no source clears the relevance floor, **do not synthesize** confident prose
    — return "I don't have anything on this in [scope]" and offer to widen scope.
    Floor is **conservative** to start (suppress only when nothing clears it),
    tunable. (Directly targets the "Casey's mom → false Jordan" fabrication class.)
25. **Per-ocean partial-failure degradation:** in a cross-ocean query, if one
    ocean's retrieval errors or times out, answer from the reachable oceans and
    **flag the failed one** ("couldn't reach Team-Eng") rather than failing the
    whole query.
26. **Stop/cancel** an in-flight stream.
27. **SSE disconnect handling:** detect a dropped stream, offer retry/resume
    instead of hanging.
28. **Rate-limit (429) UX:** the backend caps 10 req/min — surface a friendly
    "slow down" state, not a generic error.

## Amendments to ADR-0009

- **§2 (one ocean per conversation)** is superseded: a conversation now carries a
  scope descriptor (set of oceans + mode), per decision 11.
- **§9 (share read-only)** is narrowed: **cross-ocean and private-mode
  conversations are not shareable** in this build (deferred to avoid leaking
  content from oceans a recipient can't access). Existing single-ocean,
  shared-mode sharing is untouched.

## Consequences

**Backend**
- Stream request: single `graph_id` → `graph_ids: [...]`; add `model` (allowlisted),
  `style`/`depth`; `source_type` already present.
- Parallel multi-graph retrieval → merge → global rerank → **per-ocean floor**
  → ocean-count-scaled context cap.
- New **sequential** Haiku contradiction pass + a new `contradictions` SSE event.
- Honesty guard (suppress synthesis on no-clearing-hit) and per-ocean
  partial-failure handling.
- Conversation schema gains `mode` + ocean set (e.g. `conversation_oceans` join);
  turns gain a per-turn scope snapshot; **backfill** existing single-ocean rows.

**Frontend**
- Ask-page multi-select scope picker (+ private mode), model chip, "Options"
  disclosure, contradiction card, ocean badges on citations, citation side-peek,
  URL state, de-filtered conversation sidebar, contextual "Ask about this" entry
  points, stop / retry / 429 states.

**Explicitly deferred:** date-range filter; ⌘K global omnibox; recent-conversations
in the left nav; all sharing of cross-ocean/private threads; per-citation
redaction; paywalling/entitlements (every feature is available to all in this
build — only the genuine "no LLM configured" case falls back to retrieval-only);
BYOK; desktop/offline.

## Story breakdown (suggested, CS-NNN to be assigned)

1. **Backend — multi-graph retrieval core.** `graph_ids[]`, parallel per-ocean
   retrieval, merge + global rerank, per-ocean floor, scaled context cap,
   per-ocean partial-failure degradation. *(Spine — everything hangs off this.)*
2. **Backend — conversation scope model.** Scope descriptor + mode lock +
   per-turn scope snapshot + migration/backfill.
3. **Backend — contradiction detection pass.** Sequential Haiku structured pass,
   2+-ocean gate, feed into synthesis prompt, `contradictions` SSE event.
4. **Backend — model selection + honesty guard.** Allowlisted `model` field;
   empty/thin-retrieval suppression.
5. **Frontend — scope picker + private mode + URL state.** Multi-select,
   independent-but-seeded, mutually-exclusive private mode.
6. **Frontend — answer surface.** Ocean badges, contradiction card, model chip,
   Options disclosure (source filter / depth / style / regenerate / copy-export).
7. **Frontend — navigation.** Citation side-peek, contextual "Ask about this",
   de-filtered conversation sidebar.
8. **Frontend — robustness UX.** Stop/cancel, SSE disconnect retry/resume, 429
   state, honesty-guard empty states.

## Amended 2026-09-03: two-model-world tier mapping

**Status of this ADR is unchanged (Accepted).** This amendment narrows §D
(decisions 15/16/18) for the OpenRouter cutover (CoralSwarm leaving the
Anthropic API for exactly two models — `deepseek/deepseek-v4-flash-0731`
cheap-tier, `z-ai/glm-5.3-flash` capable-tier) without reopening any other
decision in this ADR:

- **Fast / Smart / Max stay capability labels, not model names.** They now
  resolve through env vars (`ASK_MODEL_FAST` / `ASK_MODEL_SMART` /
  `ASK_MODEL_MAX`, defaulting to the pre-cutover Anthropic ids) rather than
  three hardcoded ids, so the cutover is a deploy-time env flip, not a code
  change. Post-cutover: Fast → DeepSeek V4 Flash, Smart and Max → GLM 5.3
  Flash (Max differs from Smart only in its pinned reasoning effort, below).
- **The reasoning-effort dial collapsed from five rungs to four** —
  `off` / `low` / `medium` / `high` (dropping the Anthropic-only `xhigh`/`max`
  rungs; a legacy request for either still resolves to `high` rather than
  erroring) — **and is now tier-gated rather than model-name-gated:**
  - **Fast:** no dial, reasoning always off.
  - **Smart:** the full four-rung dial. **Default is now `medium`**, not
    `high` — a lower default than decision 15 originally shipped, judged
    right-sized for a cheaper capable-tier model answering by default.
  - **Max:** pinned to `high`, dial hidden — the "give me your deepest
    reasoning, no questions asked" tier.
- Server-side resolution (`backend/src/llm/model_config.rs`'s
  `resolve_synthesis_model` / `resolve_ask_tier` / `resolve_effort`) stays
  provider-neutral: nothing about the resolver names Anthropic, DeepSeek, or
  GLM — only the env vars' *values* change at cutover.
