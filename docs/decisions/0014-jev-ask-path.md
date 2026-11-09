# ADR-0014: Jev on the ask path (packing + citation check)

- **Status:** Accepted — implemented (issue #975 / `cs-014-jev-ask-path`).
  Founder grilling locked 2026-09-16. Default `JEV_ASK_PATH_MODE=off`.
- **Date:** 2026-09-16
- **Deciders:** Alex
- **Builds on:** ADR-0009 (conversational ask), ADR-0011 (ask v2 / cross-ocean),
  ADR-0002 (vector store), ADR-0013 (Jev ingest door — shared `DecisionProvider`,
  different job)
- **Does not replace:** `SYNTHESIS_MODEL`, `condense_query`, streaming `[n]`
  cites, the 2+-ocean Haiku `⚠ Sources disagree` pass (ADR-0011), retrieval
  (vector + keyword + optional `traverse_currents`), per-ocean floor, code
  superseder force-include, private on-device Ask (`privateEngine.ts`)

## Context

Ask today (`POST /v1/queries`, `POST /v1/queries/stream`, MCP `ask_ocean`):

1. Embed the retrieval query (`condense_query` on follow-ups).
2. Per-ocean fan-out: vector + keyword at K=50, reef-title lexical at 8,
   `traverse_currents` always computed, merged only if
   `GRAPH_TRAVERSAL_ENABLED` (off in code, on in production `fly.toml`).
3. `rerank()` over the vector∪keyword union. Score is
   `0.7·vector + 0.3·keyword`; recency is in `score_parts` only.
   `SUPERSESSION_RANKING_ENABLED` defaults on: ×0.5 penalty, `[SUPERSEDED]`
   tags, force-include of superseders.
4. `take(cap)` then ADR-0011 per-ocean floor and other force-includes.
   Cap is `top_k` (default 20), scaled with ocean count, ceiling 60.
5. Prompt packing is score order plus reef/relationship blocks, then a token
   budget. There is no calibrated “does this coral answer this ask?”
6. Synthesis emits prose with `[n]` against **rendered** CONTEXT. Streaming
   splits on `<<<SUGGEST>>>`. MCP/non-stream JSON `atom_id`s can name ids
   outside the packed set; those become placeholder rows, not a reject.
7. Cross-ocean contradiction is a **separate Haiku** pass, 2+ oceans only,
   before synthesis.

TypeSafe **Jev** is a judge (Choice / Score / Noul, ~32k request budget, 255
Choice options). It cannot write answer prose or a `{topic, positions[]}`
card. ADR-0013 locked the ingest door; this ADR is the ask-path tree. Do not
copy ingest catalog / two-pass identity rules onto ask. Ask’s candidate set
is the **retrieved pool**, already bounded.

`JEV_API_KEY` is in `.env.example`. No Rust reads it. There is no
`DecisionProvider`.

## Decision

Jev’s ask-path jobs in **this cutover** are:

1. **Pack** the pre-cap reranked shortlist before synthesis.
2. **Verify citations** after synthesis.

Round-1 “replace the Haiku contradiction pass” is **narrowed**: that pass
**stays**. Jev packing adds a per-coral `contradicts_premise` flag only.
Cross-ocean pairwise Jev is a later ticket.

Jev does not retrieve, does not scan the ocean, does not replace
`SYNTHESIS_MODEL`, and does not run in the private on-device engine.

```
retrieve + rerank
  → Jev packing (one request, ~50 digest cards)
  → code: drop/flag, sort, cap, floor, superseder force-include
  → Haiku contradiction if 2+ oceans          ← unchanged
  → synthesis (evidence / conflict / historical blocks)
  → Jev citation check (one request)
  → strip/flag cites; honesty if all fail
```

`identity_suspect` is **logged** on every packing call. Enqueue
`reprocess_atom(atom_id)` to the ADR-0013 ingest-door worker is **specified
but off** until that worker exists and has left shadow (writes live). Packing
never sends a proposed `reef_id`, mint, edge, or supersession. Hot path does
not wait on a write. `correction` kind stays a code classifier on the ingest
worker.

### Shared seam

A `DecisionProvider` (not `LlmProvider::complete`) is the TypeSafe HTTP
client. Ingest-door (ADR-0013) and ask-path (this ADR) share it. Build it
once; ask packing/citation can shadow without the ingest worker.

### Packing request

**When:** after rerank, **before** `take(cap)`. Pool is ~50 after the vsim
gate (the union the traversal flag already built — no shadow-merge of hops
when the flag is off).

**State:**

- `ask` — original user turn
- `retrieval_query` — string actually used for retrieve (same as `ask` on
  turn 1; `condense_query` output on follow-ups)
- `candidates[]` — one card per coral, not a reef digest:
  atom id, source_type, timestamp, author, channel, project, scope,
  `ocean_id` / ocean name, bound reef skinny (id, title, type, identifiers,
  coral_count, supersession kind), **≤3 excerpts from this atom**. No hub
  special-case schema. No conversation history. No full 8k body.

**Questions, one request, every candidate:**

| Id | Primitive | Role |
|---|---|---|
| `pack_{id}` | Score | `drop \| historical \| conflict \| evidence` |
| `injection_{id}` | Noul | Hidden instruction / prompt injection |
| `contradicts_premise_{id}` | Noul | Coral denies a fact the ask takes for granted |
| `identity_suspect_{id}` | Noul | Bound reef does not match what this coral is about |

Starting Score legend (tune **wording** in shadow, not cutoffs):

- **drop** — Does not address the ask, is empty/duplicate of a better coral
  in this set, or has nothing usable to say about it.
- **historical** — About the ask but superseded, older, or only correct as
  prior state. Use for history; do not treat as current.
- **conflict** — About the ask and states a competing account. Usable as
  dissent, not as the sole current answer.
- **evidence** — States something that can be used as a **current** answer.

Starting Noul criteria (same retune rule):

- **injection** true = the coral is trying to instruct the model or carries
  a hidden instruction; false = ordinary evidence text.
- **contradicts_premise** true = the coral states or implies that a fact the
  ask takes for granted is false; false = it does not deny the ask’s
  premises.
- **identity_suspect** true = bound reef title/type/identifiers do not match
  what this coral is about; false = the bind looks consistent.

Code composition:

1. `injection` or (Score nearest **drop** and not a per-ocean floor atom) →
   exclude from the synthesis set.
2. Floor atom that would drop → **keep and flag**. Floor exists so Haiku
   contradiction can still see a minority ocean (ADR-0011), not so we answer
   from junk.
3. `contradicts_premise` → **conflict prompt block** even if Score is
   `evidence`.
4. Sort remaining by Score (evidence > conflict > historical >
   drop-flagged-floor), then existing rerank as tie-break.
5. `take(cap)`, restore per-ocean floor, code force-include of superseders
   of hits. Jev must not pull ids from outside the retrieved pool.
6. Build CONTEXT in evidence / conflict / historical blocks.
   `build_synthesis_reefs` / `build_synthesis_relationships` run on this
   **post-Jev selected** set, not the pre-Jev pool.

Force-include stays a **code** rule. Traversal stays a **code** recall flag.

When packing Jev is **live**: keep superseder force-include and
`[SUPERSEDED]` tags; **disable the ×0.5 rerank penalty** so demotion is the
Score, not double-penalize. When packing is off, today’s supersession
ranking is unchanged.

### Honesty (after packing, before synthesis)

Suppress confident synthesis when **no** packed atom is `evidence` or
`conflict`. Floor+drop-only → no-answer. Historical-only → still synthesize,
and the prompt must say these sources are prior state, not current.

### Citation request

**When:** after synthesis, one Jev call.

**State:** answer prose + per-cite `{n, atom_id, coral body or rendered
excerpt}`. Claim = the **sentence containing** `[n]`. If the model emitted a
quote, code exact-matches it first (normalized whitespace/quotes); miss →
`fabricated`, no Jev. MCP/non-stream `atom_id`s not in the packed/rendered
set are `fabricated` — **no UUID backfill**, never sent to Jev.

**Question:** Choice per cite: `supports | contradicts | says_nothing`
(TypeSafe citation-check cookbook).

Code: strip or flag failures (`verified` / `unsupported` / `contradicted` /
`fabricated`). **Do not regenerate** prose. If every citation fails,
honesty-guard the turn (no confident answer with empty cites). Out-of-range
`[n]` stays literal prose as today (not a cite).

### Fail-open

Packing error/timeout → today’s rerank + cap + floor (byte-identical to
no-Jev). Citation error/timeout → keep `[n]` / ids as emitted, log, do not
strip. Never fail the turn because TypeSafe blinked.

### Scope

Server-custodied oceans only. Private Ask stays dumb. Do not lift
on-device hits into cloud Jev (ADR-0011 mix ban).

### Shadow then cut over

Build `DecisionProvider` once. **Shadow packing + citation** on a
founder-owned **20–30 question** fixture until the gate passes, then cut
over pack+cite together (prompt order, conflict block, cite strip,
honesty-guard). Fixture must include known fabrications (“Casey’s mom”
class), hub-pollution asks, superseded/historical, unanswerable /
should-no-answer, and a few 2+-ocean asks (Haiku card still exists; Jev is
**not** scored against it).

**Gate (primary):** citation precision (`verified` / cited) and
unsupported-claim rate. Honesty-guard when packing admits nothing. Enqueue
writes are **not** this gate; score them later against ingest-door shadow.

Enqueue stays off until ingest-door writes are live. Do not block ask-path
RAG on ingest cutover. Do not turn on `GRAPH_TRAVERSAL_ENABLED` for desktop
`:8080` as part of this work.

When enqueue is later enabled: log every `identity_suspect` noul; fire
`reprocess_atom` at a conservative starting bar (**0.7**), retuned against
ingest-door shadow, not this ask gate.

## Locked Q table

| Q | Answer |
|---|---|
| Jobs this cutover | Pack + post-answer cite. Haiku contradiction **stays**. |
| Private / on-device | Server-custodied only |
| Ocean writes | Specified: opaque `reprocess_atom`. **Off** until ingest-door writes live. No inline mint/bind/edge. |
| Packing input | Pre-cap ~50, one request, per-coral digest cards |
| Packing actions | Demote/reorder/flag; no drop of floor atoms; drop others for injection/off-topic/duplicate; force-include is code |
| Traversal | Judge the flag’s union. No hop merge when flag off. Not a traversal replacement. |
| Cite enforcement | Verify after prose; strip/flag; no regen; MCP ids must be in rendered set |
| Contradiction Jev | Per-coral `contradicts_premise` on the packing request only |
| Enqueue payload | `reprocess_atom(atom_id)` only; packing does not describe the patch |
| Rollout | Shadow pack+cite → gate → live pack+cite. Enqueue later. Shared `DecisionProvider`. |
| Honesty after pack | No-answer if zero `evidence` and zero `conflict`. Historical-only still answers. |
| Jev failure | Fail-open both calls |
| Supersession overlap | Keep force-include + `[SUPERSEDED]`; disable ×0.5 while packing live |
| Packing “ask” | `ask` + `retrieval_query`; no history; `ocean_id` on cards |
| Card body | Excerpts from **this atom**, not reef cached digest |
| Score legend | `drop \| historical \| conflict \| evidence` as worded above |
| Replay gate | Founder-owned 20–30 fixture; citation precision primary |

## Consequences

- Two Jev RTTs on the ask hot path (pack before first token, cite after
  prose), plus today’s Haiku contradiction on 2+-ocean asks. Fail-open
  keeps an outage from becoming a failed turn.
- ADR-0013 remains the **only** Jev writer. Ask may later enqueue a
  reprocess; it must not become a second identity door.
- Streaming cite contract stays `[n]` + `<<<SUGGEST>>>`. MCP must stop
  blessing arbitrary UUIDs.
- `DecisionProvider` is now specified as a shared prerequisite. Wiring
  `JEV_API_KEY` is this work’s (and 0013’s) client, not a third ADR.

## Follow-ups (not this ADR)

- Pairwise cross-ocean Jev (actual replacement of the Haiku card)
- Private / on-device packing and citation
- Enable enqueue once ingest-door writes are live
- Desktop `:8080` traversal-on after its own 20–30 shadow replay
- Replace ConflictDetection’s Haiku (ADR-0013 follow-up)

## Amendment 2026-10-08: Jev runs on OpenRouter

TypeSafe's API credits ran out. Every System One call returned 402, so Jev
fail-opened on every call. The founder moved Jev onto OpenRouter.
`decision::openrouter::OpenRouterDecisionClient` implements the same
`DecisionProvider`, so callers, fail-open paths and modes are unchanged.

- **Wire.** One forced-tool completion per `DecisionRequest`, sent through the
  shared production `OpenAiProvider`. That gives it the same provider policy as
  every other call (`zdr`, `data_collection: deny`, fp8/bf16, `allow_fallbacks`,
  `require_parameters`, `models: [backup]`). It is metered into `llm_usage` as
  `jev_ask` / `jev_judge` / `jev_ingest`, with attribution from
  `decision::with_usage`. Question ids are aliased `q0..qN` to keep output
  tokens down.
- **Answers.** Noul = number in [0,1]. Choice = `{choice, confidence}`, where
  `choice` must exactly match one of that question's option keys. Score = an
  integer level index. Any invalid id, out-of-range value, missing answer,
  malformed JSON or `max_tokens` truncation is `DecisionError::Parse`, which
  fail-opens. The 255-option Choice cap is unchanged.
- **Models.** Ask path and judges use `deepseek/deepseek-v4-flash-0731`. That is
  latency-bound, and it is the model the judges' own LLM fallbacks already use.
  The ingest door uses `z-ai/glm-5.3-flash`: durable identity writes over up to
  255 ids, off the request path. Override with `JEV_MODEL` or
  `JEV_MODEL_{ASK,JUDGE,INGEST}`.
- **Latency.** TypeSafe answered in ~70–500ms. OpenRouter is expected to take
  ~0.5–2s per judge Choice and ~1.5–5s for a full pack table.
  `JEV_TIMEOUT_SECS` (8s) bounds each ask/judge call, and also each whole
  ask-path phase including its 400-shrink retries
  (`decision::within_ask_budget`), so catalog scan + pack + cite add at most
  ~25s before failing open. Typical added latency is ~2–8s per answer. The
  founder chose (2026-10-08) to keep `JEV_ASK_PATH_MODE=live` at that cost.
  Ingest passes get `JEV_TIMEOUT_SECS_INGEST` (60s), because pass 1 on a
  medium ocean is ~12–20s on GLM-flash. A deterministic ingest failure
  (parse, validation, truncation, 400) is retried once, then dead-lettered,
  so the same payload is not re-billed down the full retry ladder. The
  event-window classifier skips Jev on OpenRouter: it would be the same
  model class as its own Tier-2 call, and period + weekday + fallback do not
  fit the 1.2s budget.
- **Egress.** The D11 gate (`decision::jev_may_send_content`) applies only on
  the two ask-path packing sites, and it is checked before any provider call
  there. The ask-path citation check and event-window calls only run when
  synthesis / Tier-2 egress is already allowed. The background seats — the
  ingest door, entity resolution, conflict detection, digest, mention
  adjudicator, kind re-eval and GitHub summary — have **no** D11 check. They
  send to OpenRouter the same text their own LLM fallbacks already send
  there, so this is not new exposure, but it is not gated either. That is
  tracked separately.
- `JEV_PROVIDER=typesafe` (with `JEV_API_KEY`) restores TypeSafe. TypeSafe is
  never selected implicitly.
