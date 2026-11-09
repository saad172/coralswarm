# ADR-0006: The Ocean Model — Primary, Team, and Organization Oceans + Promotion

**Date:** 2026-06-13
**Status:** Accepted — **Implemented 2026-06-14** (migration 0026; store `promote_node` +
`Graph.kind`/`is_primary`; `POST /v1/nodes/{id}/promote`; web ocean switcher + promote control)
**Deciders:** Alex
**Builds on:** ADR-0004 (local-first / server-trust), ADR-0005 (local store, sync, encryption)

---

## Context

"Ocean" is the product's word for a graph + a visibility boundary (lexicon: coral = atom,
reef = node, ocean = graph/scope, currents = edges/activity). Until now the term has been used
loosely: the data model has `graphs.kind ∈ {personal, team}` and atoms/nodes carry
`scope ∈ {private, account}`, but there is no crisp definition of what oceans exist, how they
relate, or how knowledge moves between them. The marketing site already mocks a "promote a reef to
the team ocean" flow (`marketing-web/components/sections.tsx`), but no such concept is modeled in
the backend, sync engine, or app.

This ADR fixes the definition before the team/promotion features get built, so the data model,
sync ops, and UI all share one mental model.

## Decision

### 1. Every user has exactly one **Primary ocean** — private, the default

- Created on first run; **one per user.** It is *the* ocean — everything you ingest lands here first.
- **Private by definition.** Nothing in it is visible to anyone else. This is the local-first,
  on-device graph (ADR-0004): your keys, your data, fully usable offline.
- It is the **source of truth for everything you author.** Promotion never removes anything from it.
- Maps to `graphs.kind = 'personal'`, with one such graph per user designated **primary**
  (net-new: today no graph is marked primary; all personal graphs are treated uniformly).

### 2. **Shared oceans** are promotion targets — Team (many) and Organization (one)

Shared oceans widen visibility. There are two breadth levels, and they differ in **cardinality**:

- **Team ocean** — a named group you create or are invited to; members see what's promoted into it.
  **There can be many team oceans** per organization, and a user can belong to several.
  Maps to `graphs.kind = 'team'` + `team_members`.
- **Organization ocean** — the widest shared ocean: everyone in the company.
  **Exactly one per organization** (singleton). Net-new: today the `account` is the implicit
  boundary with no org-level graph.

Both are the same graph primitive carrying a visibility level; the org ocean is the singular,
widest level, while team oceans are plural and selective.

### 3. **Promotion** is explicit, one-way (widening only), and copies — it never moves

- **Direction is strictly one-way:** Primary → Team → Org. Promotion only ever *widens* visibility.
  There is **no pull-down** in v1: knowledge does not flow from a shared ocean back into a primary.
  (This mirrors the "nothing enters your ocean without an explicit action" ingest principle.)
- **The original stays in your primary ocean.** Promotion **publishes a copy** into the target
  shared ocean's graph, tagged with provenance (`promoted_by`, `promoted_at`, source version). Your
  primary remains the authoritative origin.
- Because the original is retained, a promoted reef can be **withdrawn** from a shared ocean without
  the author losing it, and **offboarding** (already half-designed in `web/app/(app)/team/page.tsx`)
  keeps a departed member's promoted contributions in the shared ocean while their primary ocean
  leaves with them.

### Visibility ladder

```
  Primary ocean            Shared oceans (promotion targets)
  ───────────              ──────────────────────────────────────
  ┌─────────────┐          ┌──────────────┐        ┌──────────────┐
  │   PRIMARY   │  promote │  TEAM ocean  │ promote │  ORG ocean   │
  │  (private)  │ ───────▶ │  (a group)   │ ──────▶ │ (everyone)   │
  │  on-device  │          │  MANY, opt-in│         │  ONE per org │
  └─────────────┘          └──────────────┘        └──────────────┘
   one per user             widen-only, copies published with provenance
   source of truth          (originals always remain in the primary)
```

## What this changes vs. today

| Concept | Today | Net-new for this model |
|---|---|---|
| Primary designation | all personal graphs equal | mark one personal graph as **primary** per user |
| Scope values | `private`, `account` | add `team` and `org` (widening levels) |
| Org ocean | none (account = boundary) | one org-level graph per organization (singleton) |
| Multiple teams | 1 account → 1 team → 1 graph | **many** team oceans per org; users in several |
| Promotion | none (word reused for decision-confirm) | explicit **copy-publish** op + provenance (`promoted_by`, `promoted_at`, `from_version`) |
| Flow direction | n/a | **strictly one-way** (widen only); no pull-down in v1 |

The encryption and sync plumbing already supports this shape — per-graph oplog
(`op_log.graph_id`), team-key envelopes wrapped per member (`team_member_key_envelopes`) — so this
is an extension, not a rebuild.

## Rationale

- **One private home, many ways to share.** A single, always-private primary ocean keeps the
  privacy promise simple and absolute; sharing is always a deliberate widening, never a default.
- **Copy-on-promote fits local-first.** The author's device stays the source of truth; shared
  oceans hold durable, provenance-tagged copies that survive the author going offline or leaving.
- **One-way keeps the model legible.** Widen-only promotion avoids copy-provenance loops and
  cross-ocean merge conflicts in v1; pull-down/subscribe can be revisited later if needed.
- **Teams plural, org singular** matches how companies actually organize: many overlapping teams,
  one company-wide commons.

## Consequences

**Positive:** crisp, teachable mental model; privacy is absolute at the primary tier; promoted
knowledge is durable and attributable; builds on existing team + sync infrastructure.

**Negative / risks:** copy-publish means a promoted reef and its primary original can diverge —
we must decide whether/how edits to the origin re-publish (out of scope here; default: re-promote
to update). Withdrawal and offboarding semantics need their own spec. Org-ocean access control
(who's "in the org") is net-new and must be defined when the org tier is built.

## Implemented (2026-06-14)

- **Schema** (`backend/migrations/0026_ocean_model.sql`): `graphs.kind` widened to
  `personal|team|org`; `graphs.is_primary` + partial unique indexes enforcing one primary
  and one org ocean per account; `node_promotions` provenance table.
- **Store** (`backend/src/store/`): `Graph.kind`/`is_primary`; `create_graph(kind)` auto-marks
  the account's first personal ocean as primary; `promote_node` (Postgres + libsql) publishes a
  copy keyed by `(graph,type,title)`, stamps the target scope, records provenance, emits a
  node-upsert sync op, and is idempotent on re-promote.
- **API**: `POST /v1/nodes/{id}/promote` (IDOR + shared-ocean validation); `kind`/`is_primary`
  surfaced on the graphs endpoints; `POST /v1/graphs` accepts `kind` (personal|org) with a 409
  org-singleton guard.
- **Web**: ocean switcher badges primary/team/org; reef detail has a one-way promote control.

## Out of scope (follow-ups)

- Edit-after-promote / re-publish semantics (today: re-promote to update the copy).
- Withdrawal and offboarding flows.
- Org-ocean membership and access control.
- An API to create team oceans without going through the teams (key-envelope) flow.

## References

- ADR-0004 (local-first / server-trust); ADR-0005 (local store, sync, encryption)
- Lexicon: `ONBOARDING.md`; ocean-lexicon memory
- Existing data model: `backend/migrations/0003_atoms.sql`, `0011_nodes.sql`, `0022_teams.sql`,
  `0023_sync_oplog.sql`
- Marketing promotion mock: `marketing-web/components/sections.tsx`
