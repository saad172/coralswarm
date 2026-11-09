# ADR-0008 — Relationship Knowledge Graph: Entity Resolution + Automatic Inference

**Status:** Proposed (design — grilled with founder 2026-06-19)
**Context branch:** `worktree-cs-relationship-inference`

## Problem (grounded in live prod data)

Querying the ocean about people fails or lies. Confirmed against live `coralswarm-db`:

**Atoms (the actual stored facts):**
- "Casey is my daughter" / "Casey is my 16 year old daughter"
- "Harper is my wife"
- "Casey loves playing with her Cousin Kacey Lane"
- "Jordan is my older sister who lives in atlanta…"
- "Jordan is the mother of **kacey**"  ← Kacey, the cousin — **not** Casey

**Correct answers:**
- "Who is Casey's mom?" → **Harper** (2-hop inference: Harper —spouse→ Alex —parent→ Casey). *No atom states this.*
- "Who is Kacey's mom?" → **Jordan** (stated).

**What the system actually did, and the 5 root causes:**

| # | Bug | Evidence | Impact |
|---|-----|----------|--------|
| 1 | **Kacey/Casey conflation** | "Jordan is mother of kacey" answered as "Jordan is **Casey's** mother [2]" | Confident **false** cited answer |
| 2 | **No multi-hop traversal** | `Harper` reef and `Casey` reef share **0 atoms, 0 edges** — only both touch the `Alex Example` hub | Inference impossible; data never co-retrieved |
| 3 | **No typed relationship edges** | "wife"/"daughter"/"mother of" exist only as free text in atoms | Nothing to traverse |
| 4 | **Duplicate reefs** | Two identical `Casey` reefs (`82f97a34`, `40bfef43`) | Nondeterministic recall across phrasings |
| 5 | **Split author identity** | Atoms authored as both `Alex Example` and raw Slack id `U57C3A9EF09` | "my X" can't resolve to a person |

## Decisions (founder-confirmed via grill)

1. **Goal:** ship real, general kinship/graph reasoning — typed edges + multi-hop traversal, for *all* relationship types, not just family. Durable product capability, not a demo patch.
2. **Inference is materialized at write time.** Inference runs automatically on ingest and **stores** derived edges; not re-derived per query.
3. **Human-in-the-loop, not hard assertion.** Inferred edges are *proposals*. Nothing speculative is ever stated as hard fact. Resolves the defeasibility trap (wife+daughter→mother is not logically valid — step/prior-marriage/adoptive cases).
4. **Full entity resolution is in scope** — it is the substrate. Dirty nodes ⇒ inference manufactures garbage (bug #1). Includes reef dedup, near-name disambiguation, and author↔person unification.
5. **ER mechanism = evidence-based LLM adjudication.** Block candidates by normalized-name/embedding, then an LLM adjudicator compares **attributes + relationships** (birthdate, location, connections) to decide same/different + confidence. Separates Kacey (May 15/Atlanta/Jordan's child) from Casey (Feb 6/Alex's daughter); merges the two Caseys. Above threshold → merge; ambiguous → review.
6. **Engine runs server-side now, on-device later.** Build in the backend worker (Claude API), matching today's deployed extraction path and the live data. Known debt: must port to the on-device engine for true E2E private oceans (per ADR-0004/0007). Tracked, not solved here.
7. **Confidence gate, single threshold.** Each candidate edge carries a confidence. Above the gate → auto-confirmed. Below → review queue. (Plan adds a soft floor so the truly-low-confidence tail doesn't firehose review; default behavior otherwise is "review everything below the line.")
8. **Tiered answering + conversational confirmation.** Stated/confirmed edges answer as fact. High-confidence inferred-but-unreviewed edges answer **hedged** with provenance ("Harper is *likely* Casey's mother — inferred from: she's your wife + Casey is your daughter; unconfirmed"). Below-threshold edges aren't used in answers, only in review.
9. **The ocean becomes a conversation.** Shift query from one-shot Q&A to stateful chat so the bot can confirm inferences **inline** ("I think Harper is Casey's mother — right?"). User's reply mutates edge status in real time; batch review section remains as the async path. Both routes converge on the same edge-status state machine.

## Architecture sketch

### Data model — `relationship_edges`
```
relationship_edges(
  edge_id        uuid pk,
  graph_id       uuid,                -- ocean scope (private/team/org inherited)
  src_node_id    uuid,               -- canonical reef
  predicate      text,               -- from predicate registry
  dst_node_id    uuid,
  origin         text,               -- 'stated' | 'inferred'
  status         text,               -- 'suggested' | 'confirmed' | 'rejected'
  confidence     real,
  provenance     jsonb,              -- stated: atom_ids; inferred: derivation chain (rule + source edge_ids)
  created_at, updated_at
)
```
- **Inverse/symmetry materialized** (parent↔child, sibling/spouse symmetric) so traversal is direction-agnostic.
- **Rejected edges persist** as negative constraints (don't re-suggest) — mirrors existing `non_supersession_pair`.
- **Sync:** new oplog op types for edge upsert/status + node merge; edges inherit ocean scope. Integrates with the Phase-3 sync engine.

### Predicate registry (extensible)
Curated predicates with declared algebra: `inverse`, `symmetric`, `transitive`, and **composition rules**.
- v1 domains: **kinship** (spouse, parent/child, sibling, cousin, grandparent…) + **professional** (reports-to, manages, colleague).
- Key composition rule for the headline bug: `spouse(A,B) ∧ parent(B,C) ⇒ parent(A,C)` tagged *social/defeasible, medium confidence*.
- Free-text relationships still stored as edges, but **no auto-inference** until a predicate is promoted into the registry.

### Pipelines
1. **Stated-edge extraction** (extend `extract.rs` + a new prompt): pull subject-predicate-object triples from each atom → `origin='stated'`. Resolve "my X" via author→person identity.
2. **Entity resolution** (new worker stage): blocking → LLM adjudication → merge (canonical pointer, oplog-logged, reversible) or queue.
3. **Inference closure** (new background reasoner): runs over the affected neighborhood when edges/nodes change. Applies registry rules → `origin='inferred'` with derivation chain + confidence. Confidence gate splits auto-confirm vs review. Bounded by rule whitelist + max hops + dedup against stated/rejected.
4. **Retrieval/synthesis**: entity-centric queries expand the **typed-edge subgraph** (depth 2 for kinship) and pass edges *explicitly* into synthesis context. Tiered answering per Decision 8.
5. **Conversational layer** (Decision 9): stateful session; synthesizer may emit a confirm-question; user reply → edge status mutation.

## Phasing (proposed — sequencing deferred by founder)

> Sequencing intentionally left open (grill, 2026-06-19). Options weighed: ER-first (clean but slower payoff) · thin vertical slice for the one family · ER+stated-edges only (defer inference). Decide before build.

- **Phase 0 — Entity resolution.** Dedup reefs, author↔person unification, Kacey/Casey split. *Hard prerequisite; nothing trustworthy without it.*
- **Phase 1 — Stated typed edges + traversal.** Extract stated relationships; multi-hop subgraph retrieval; tiered (non-hedged, stated-only) answers. "Who is Kacey's mom?" → Jordan, reliably.
- **Phase 2 — Inference closure + review queue.** Registry rules incl. spouse+parent→parent; confidence gate; review UX. "Who is Casey's mom?" → "likely Harper (unconfirmed)".
- **Phase 3 — Conversational confirmation.** Stateful chat; inline confirm loop folds back into edge status.

## Open sub-decisions (defaults chosen, confirm before build)

- **D-a — Confidence thresholds.** Default: auto-confirm ≥ 0.85; review 0.5–0.85; soft-hide < 0.5 (logged, not surfaced). *Tune on real data.*
- **D-b — Merge mechanics.** Default: non-destructive canonical-pointer/alias (keep both rows, mark canonical, re-point reads) over hard delete — reversible + sync-friendly. Reuse supersession-style infra.
- **D-c — v1 rule domains.** Default: kinship + professional only; social/other stored but not inferred.
- **D-d — Conversational scope.** Default: Phase 3, after the graph mechanics are solid (don't block edge inference on a chat rewrite).
- **D-e — On-device port.** Default: tracked as debt, not in this epic.
```
