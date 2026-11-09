# ADR-NNNN — Reef Subtypes, Relationship Aboutness, and the Entity-Resolution Substrate

**Status:** Proposed (design) + **Partially implemented** (the ER substrate fixes below are built on
branch `cs-er-substrate-fixes`; the subtype + aboutness work is designed only).
**Number:** assign at merge — `develop` carries unmerged ADRs up to 0009; pick the next free number
then (this branch was cut from `origin/main`, which has none of the `docs/decisions/` series yet).
**Builds on:** ADR-0006 (Ocean model), ADR-0008 (Relationship knowledge graph).
**Deciders:** Alex.

---

## Context

Reviewing "reefs" (knowledge-graph nodes; lexicon: coral=atom, reef=node, ocean=graph, currents=edges)
surfaced that the graph is, in practice, **a people-and-coworkers map with everything else floating
unattached**, standing on an entity-resolution substrate that *destroys relationships when it tidies
up*. This ADR records three layers of decisions reached by grilling, and the subset already
implemented.

The key reframe: the original prompt was "we need more reef types." The answer is **no new top-level
reef types** — the existing `entity` reef already carries four sub-kinds the extractor produces and
then discards (`map_node_type` collapses person/org/topic/project → `entity`). What's needed is to
*un-flatten* that, then connect the non-person reefs, then make the substrate trustworthy.

---

## Layer 1 — Reef subtypes (DESIGN ONLY; not yet built)

1. **No new `nodes.type` values.** Add a `subtype` column to entity reefs: `person | org | topic |
   project | other`. The LLM already emits these; stop discarding them. Off-list values **clamp to
   `other`** so filter tabs stay a closed set.
2. **Subtype is a label/filter, not identity.** Identity stays `(graph, type, title)`. Conflicts when
   extractions disagree resolve **sticky-first with a person-upgrade exception** (a reef later seen as
   `person` upgrades; other jitter is ignored). Majority-vote deferred.
3. **Backfill** via a *scoped* re-extraction job that runs the real extraction call for context but
   **writes only the subtype column** — no new decisions/reefs, no version churn.
4. **UI:** subtype filter chips *inside* the Entities tab (not four new top-level tabs); a **retype**
   control reusing the existing rename plumbing (so a wrong sticky subtype is correctable).

## Layer 2 — Relationships between reefs (DESIGN ONLY; not yet built)

The relationship layer (ADR-0008) is rich but **person-only and type-blind**. Three decisions:

1. **Q1 — Predicate endpoint signatures.** Each predicate/composition rule declares the subtypes it may
   connect (`parent_of` = person→person). `infer()` (today a pure UUID join with no endpoint check, and
   the LLM defeasibility review is **off by default**) **skips composing when both endpoints are
   confidently typed and violate the signature** — unknown/`other` ends pass through. A zero-LLM
   structural guard so mechanical inference can't manufacture `spouse_of(Rust, GDPR)`.
2. **Q2 — Aboutness layer.** Add `about` / `decided_by` edges (decision/thread/reference → entity),
   **derived from shared-atom co-membership** (no new LLM). Reuses the existing `relationship_edge`
   table. Decision↔decision **supersession** (ADR-0006) stays its own logical/temporal system.
3. **Q3 — Topics/projects earn edges.** They become aboutness *targets* for free via Q2. Add
   `works_on` (person→project) as an **extracted** (not co-occurrence-inferred) predicate so
   "who's working on X?" returns workers, not everyone who name-dropped X. Project→project structural
   links (`depends_on`) deferred.
   - **Correction (D11, 2026-07-28):** this bullet is stale. It originally warned that
     `build_synthesis_relationships` resolved endpoint titles via `list_nodes(..., Some("entity"),
     ...)` — entity-only — so an aboutness edge with a decision/thread end would be silently dropped
     before reaching an answer. **That is no longer true.** Dogfooding gap-sweep #2 item 7 (2026-07-18)
     replaced that call with a targeted, type-agnostic lookup — `store.nodes_by_ids` /
     `nodes_by_ids_scoped`, i.e. `SELECT node_id, title FROM nodes WHERE graph_id = $1 AND node_id =
     ANY($2)` — resolving every endpoint id the retrieved edges actually reference, regardless of
     `type`. Verified against `backend/src/routes/queries.rs`'s `build_synthesis_relationships` as of
     this correction. **Q2/Q3's aboutness work is therefore not blocked on this** — the answer path
     already resolves non-entity endpoints; only the aboutness edges themselves (Q2) remain to be
     built.

## Layer 3 — Entity-resolution substrate (IMPLEMENTED on `cs-er-substrate-fixes`)

ER is the hard prerequisite ("dirty nodes ⇒ inference manufactures garbage"). Review found three live
defects; all three are fixed on this branch.

### Bug #1 — `merge_nodes` cascade-deletes relationship edges (data loss)
`relationship_edge.{src,dst}_node_id` are `ON DELETE CASCADE`; the merge hard-deletes the dup node, so
**every edge touching it was silently destroyed** (atoms were re-pointed; edges were not). Supersession
and promotions were *deleted* too.
**Fix:** before deleting the dup, **re-point** edges (+ supersession + promotions) dup→keep, deduping
`UNIQUE(graph, src, predicate, dst)` collisions and dropping `CHECK(src<>dst)` self-loops; emit the
matching sync ops (`relationship_edge` upsert/delete, `supersession_edge` upsert/delete; promotions are
server-only and not synced).

### Bug #2 — auto-merge is irreversible and self-reinforcing
Merges run automatically (confidence ≥ threshold), hard-delete the dup, **and teach the survivor the
dup's name as an alias** — so a wrong merge (the Kacey/Casey conflation ADR-0008 exists to kill) is
unrecoverable *and* self-amplifying.
**Fix — undo-log (`node_merges`, migration `0048`):** within the merge transaction, snapshot the dup
row + the atoms actually moved + each edge re-pointed/dropped with its original endpoints. New
`Store::unmerge_nodes(merge_id)` restores the dup exactly (idempotent, `FOR UPDATE` + `undone_at`
guard), plus `POST /v1/nodes/merges/{id}/undo`. Survivor stays the single live row → **no read-path
tax** (rejected the ADR-0008 D-b canonical-pointer tombstone for that reason). Auto-merge threshold and
alias-learning unchanged.

### Bug #3 — author identity isn't unified across sources
The same human appears as a Slack display name in one atom and a raw `U…` id in another, so first-person
edges ("my wife" / "my daughter") anchor to **different** author reefs and the headline inference
(`spouse_of ∘ parent_of ⇒ parent_of`) can't compose. Title-based ER can never reconcile id-vs-name.
**Fix (migration `0049`):**
- **(B)** `connector_identities` directory `(provider, workspace_id, external_user_id) → canonical_name`,
  consulted at Slack ingest before re-deriving; a raw-id fallback (e.g. on a `users.info` outage) is
  **never persisted as canonical**, so a transient failure can't corrupt the directory.
- **(C)** `graphs.self_node_id` — a per-ocean "self" reef the owner's first-person binds to; lazily
  created, FK `ON DELETE SET NULL`, title sticks once set.

**Known edge case:** if the *first* owner-authored atom arrives during a `users.info` outage, the self
reef bootstraps with the raw id as its title and that sticks (mitigated by retype). Worth a one-liner
in release notes.

---

## Implementation status & verification

**Built (this branch):** Layer 3 only — bugs #1/#2/#3. Migrations `0048`, `0049`; both store backends
(Postgres `mod.rs` + libsql `libsql.rs`) mirrored; `routes/nodes.rs` undo route; `routes/slack.rs`
ingest binding; `store/sync.rs` `relationship_edge_delete_op`. Bonus: fixed a latent libsql RETURNING-
cursor bug in `upsert_relationship_edge` that broke commits inside multi-statement transactions.

- ✅ `cargo check` clean; **548 lib tests pass** (incl. new tests: edge survives merge re-pointed,
  dup↔keep edge dropped not self-looped, colliding edge deduped, unmerge restores node+atoms+edges,
  author collapse, self-reef binding).
- ⚠️ **Not run against a live Postgres** — PG paths verified by `cargo check` + parity with the
  unit-tested libsql impl. Migrations `0048`/`0049` need a real apply.
- ⚠️ End-to-end ingest→extract→multi-hop ("who is Casey's mom → Harper") not run live.

**Live-DB checklist before merge:**
1. Apply `0048`, `0049` to a Supabase branch DB; confirm forward + `Down:` blocks.
2. Seed two duplicate reefs with relationship edges; run ER merge; assert edges survive re-pointed and
   a `node_merges` row exists.
3. `POST …/merges/{id}/undo`; assert the dup + its atoms + its edges return.
4. Ingest two Slack atoms from one user under different name forms; assert one author reef.

---

## PR / story split (per the founder-approval workflow)

These are independent enough to land as **three PRs**, in this order:

1. **CS-ER-1 — Merge edge-integrity.** Bug #1 only: re-point edges/supersession/promotions on merge +
   the libsql cursor fix. Smallest, highest-urgency (active data loss). No migration.
2. **CS-ER-2 — Reversible merge.** Bug #2: `0048` `node_merges`, `unmerge_nodes` (both stores), undo
   route. Depends on CS-ER-1 (re-pointing is what the undo-log records).
3. **CS-ER-3 — Author identity.** Bug #3: `0049`, connector-identity directory, self reef, Slack ingest
   binding. Independent of #1/#2.

Layers 1–2 (subtypes, aboutness) are **separate future stories** — they are designed here but not built,
and should not be implied as shipped.

## Out of scope / deferred
Majority-vote subtype resolution · project→project structural edges · multi-hop aboutness at query time
· conversational inline edge confirmation (ADR-0008 Phase 3) · on-device port of the inference engine ·
porting the connector-identity directory to desktop (currently a server-only no-op stub).
