# ADR-0013: Jev at the ingest door

- **Status:** **`IngestDoor` job kind retired 2026-10-18** (founder decision,
  source-agnostic pipeline PR 0b): the shadow worker, its embed-time enqueue
  and the `JobKind` variant are deleted (tenant 0160 / control 0327 keep
  dead-lettering hand-inserted rows). Jev itself stays: the decision provider,
  Ask-time Jev and the judge are unchanged, and the identity seat moves into
  extraction v2. `JEV_INGEST_DOOR_MODE` is still read by
  `decision::identifier_auto_merge_enabled` (EntityResolution), so the flag
  stays until that worker is restored. Previously: Accepted — **shadow worker
  implemented** (branch `cs-013-jev-ingest-door`). Founder-confirmed 2026-09-16 in a
  grilling session. Do not re-grill the ingest tree; implement against this
  document or amend it. Live reef/edge writes are still off.
- **Date:** 2026-09-16
- **Deciders:** Alex
- **Builds on:** identity door (`docs/plans/identity-resolution-at-the-door.md`),
  ADR-0008 (relationship knowledge graph),
  `docs/decisions/reef-subtypes-and-er-substrate.md`,
  graph-temporal-memory workstream B (`docs/plans/graph-temporal-memory.md` §3)
- **Does not replace:** `SYNTHESIS_MODEL`, extraction prompts, kind-reeval,
  graph-wide ER, ConflictDetection, anchored chat corrections, desktop
  title-upsert

## Context

Ingest today: per-coral Haiku entity extract and relationship extract in
parallel, a deterministic identity door (identifier → title/alias →
permutation → near-match), later ER, kind-reeval every +100 corals, and
ConflictDetection for claim pairs. TypeSafe **Jev** is a System One model
(Choice / Score / Noul, ~32k shared request budget, 255 Choice options). It
cannot generate names or triples. `.env.example` has `JEV_API_KEY` but nothing
in Rust reads it.

Jev’s two product jobs (2026-09-16 lean, still true): **(1) ingest door** —
this ADR — and **(2) ask packing + citation check** — not this ADR.

## Decision

Jev is the ingest-door **judge**. Cheap extract still **proposes** mention names
(≤6), non-catalog relationship triples, and claims (≤6). Pass 3 catalog slots
for `works_on` / `manages` / `member_of` are code-built, mention-gated, and
Jev-scored — extract does not invent those three predicates. Code owns mint
titles, `correction` kind, timestamps, and human vetoes.

**One sequenced job per new coral.** Shadow first: run and log; **write
nothing**. No backfill. Desktop’s old title-upsert path is out of scope until
this worker exists.

```
Haiku extract (existing prompts)
  → catalog
  → Jev pass 1  skinny identity recall
  → Jev pass 2  identity + entity_type
  → Jev pass 3  relationships
  → Jev pass 4  claims + supersession
```

The deterministic door still **computes** identifier / exact title / alias /
permutation 1.0 and stamps `door_hit: {reef_id, how}` into Jev state. It does
**not** bind. Every mention, including exact email/title, goes through Jev so
a wrong exact title (`Alex` fragment vs `Alex Example`) can be refused.

### Catalog (once per coral)

Union of:

- reefs with `coral_count > 0` or an identifier
- this mention’s trigram **or** embedding neighbors even at 0 corals
- reefs already on this coral
- author / self reef

Zero-coral ghost reefs stay out. Trigram is a **recall source**, not a veto.

### Pass 1

Skinny cards in state (id, title, type, aliases). Per mention, Choice in
chunks of **254 ids + `none`**, all mention×chunk questions in **one**
request. Option descriptions are null (ids live in state). A reef reaches
pass 2 if any chunk is not `none`. No `"new"`, no mint.

### Pass 2

Digest cards for survivors only, **same schema for hubs**:

id, title, `entity_type`, aliases, identifiers, coral_count / degree, cached
digest `{summary, excerpts≤3}`, ≤5 currents as `predicate → title`.

**One Choice per mention:** survivor ids + `new` + `curator` (mutually
exclusive — not independent Scores). Same request: `entity_type` Choice
`person | org | topic | project`. Sticky human PATCH is not overwritten.
`nodes.type` is not written. Kind-reeval stays (it sees the accumulated reef;
pass 2 sees this coral).

Composition rules:

- Choice `new` and `door_hit.how` is **identifier** → force **curator**, no mint
- `"new"` against title / alias / permutation **may mint** (escape ghosts)
- Reroute = picking another survivor
- Mint title = Haiku `name` / `surface_form`

### Pass 3 (after binds)

Titled catalog slots: **author reef × each other bound entity** on this coral,
predicates `works_on` / `manages` / `member_of`. State carries `title` on each
slot. **Mention-gate:** code-drop (compose Drop, do not ask Jev) when neither
the reef title nor the bind mention name appears in the coral.

Jev 3-level Score **drop / Suggested / Confirmed** on remaining slots. Extract
does **not** propose these three predicates — Jev judges the catalog. Other
extractable predicates still come from cheap extract; unbound endpoint → drop.

Inference / `expert_in` workers unchanged.

### Pass 4 (after pass 2; needs bound entity ids)

New Haiku claims: same drop / Suggested / Confirmed Score.

Then per surviving claim, shortlist old decisions: **rarest bound entity →
cosine neighbors → cap 20**, digest cards, minus `non_supersession_pair`.
Per pair, Score **unrelated / same-fold / supersede / curator**.

- same-fold: attach coral, no mint
- supersede: direction = later `decided_at`
- **kind is code** (`classify_supersession_kind`); Jev never emits `correction`
- `non_supersession_pair` blocks the write

### Other writers

| Writer | Fate |
|---|---|
| ER | Stays (historical fragments, promotion). Identifier auto-merge **off** in shadow and after cutover. |
| ConflictDetection | Stays as cleanup. Replacing its Haiku with this Score is a later ticket. |
| Anchored chat corrections | Stay **deterministic** (`anchor_atom_ids`). Jev does not re-judge. |

### Shadow

New ingest only. Compare Jev outcomes to live door + Haiku writes. Tune
**level wording**, not probability cutoffs. No live reef / edge writes until
cutover.

### Locked Q table

| Q | Answer |
|---|---|
| Shortlist | Two-pass; catalog per coral; trigram is recall not veto |
| Pass-2 card | Digest-shaped, no hub-special casing |
| Pass 1 >255 | Chunked Choice, 254 + `none` |
| Door bind | Jev sees every mention; door computes hint only |
| Door vs Jev | Auto-reroute; identifier+`new` → curator; `door_hit` in state |
| Type | `entity_type` on pass 2; not `nodes.type`; kind-reeval stays |
| Mentions | Haiku proposes; Jev resolves |
| Relationships | Titled catalog slots + mention-gate; extract triples for other predicates |
| Edge status | Score drop / Suggested / Confirmed |
| Job topology | One sequenced worker |
| High/mid/low | Score levels, not fitted 0.85 bars |
| Extra pairs | Author × others, three predicates, titles in state, mention-gate |
| Claims | In this cutover (pass 4) |
| Rollout | Shadow new ingest; ER stays; identifier auto-merge off |
| Claim kind | Code classifier, not Jev |
| Old-claim recall | Rarest entity + cosine, cap 20 |
| ConflictDetection / anchors | A: door-time Jev; those writers stay |
| Claims request | Pass 4, not mixed into pass 3 |

## Consequences

- ExtractEntities ∥ ExtractRelationships race is illegal; one worker sequences
  Haiku then four Jev requests.
- False mint of identifier-shaped mentions is a curator item, not an auto-split.
- Ask packing / citation check is **ADR-0014**. Do not smuggle it into
  this worker.
- A `DecisionProvider` (not `LlmProvider::complete`) is the intended Rust
  seam for Jev; this ADR does not specify the type. **Built** in
  `backend/src/decision/` (shared with ADR-0014). The ingest-door worker
  is `backend/src/workers/ingest_door.rs`, job kind `IngestDoor`, env
  `JEV_INGEST_DOOR_MODE=shadow`. Identifier auto-merge is paused while that
  mode is shadow or live.

## Follow-ups (not this ADR)

- Ask-path Jev — **ADR-0014** (packing + citation check; Haiku
  contradiction stays)
- Replace ConflictDetection’s Haiku with the pass-4 Score
- Desktop extract path
- Backfill
- `JEV_API_KEY` / TypeSafe HTTP client wiring — **done** (`DecisionProvider`,
  shared with ADR-0014). Ingest-door live writes remain a follow-up.

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
- **Ingest door ships in `shadow`.** Production `fly.toml` sets
  `JEV_INGEST_DOOR_MODE=shadow`. Ingest binds are durable merges. Strict
  validation rejects an id outside the option set, but it cannot catch a
  valid-but-wrong pick, and pass 1 does not gate on confidence. The door goes
  `live` only after its shadow logs show high agreement with a hand-checked
  sample of atoms.
