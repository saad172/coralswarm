# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Users

Knowledge workers exploring personal and company knowledge. More specific roles, organization sizes, and permissions are undecided.

The user confirmed two equally important jobs:

- Explore what came out of a meeting or coding session.
- Understand a project and its history.

## Product Purpose

CoralSwarm connects meaningful knowledge to its source and to the people, projects, and other things it concerns. Success means understanding what happened, how it relates to earlier work, and what evidence supports that understanding.

## Positioning

The approved product model makes each atom one meaningful fact, decision, action, or observation. An atom links to an exact source passage, resolves to canonical nodes, and participates in a connected temporal graph across personal and company data. This describes the intended model; it is not a claim that every existing record already satisfies it.

## Operating Context

Users enter through recognizable meetings, coding sessions, and projects. They need to move between session outcomes, project history, related knowledge, and source passages without losing their place.

The existing **Library** is the navigation and browsing baseline. The user rejected the proposed standalone Memory Explorer concepts and asked for an experience similar to Library now, enhanced. Enrich familiar items and details with outcomes, source evidence, project history, and contextual cross-links. A new primary navigation destination is not the requested direction.

The agreed Library landing is **All sources**, showing recent captured material alongside prominent projects and work topics. Prominence reflects meaningful connections and breadth of supporting sources; recency remains a separate signal. Exact ranking weights and a production score remain unimplemented design choices.

There is no top-level Topics tab. **View all nodes** opens all node kinds, and one Library search distinguishes matching nodes from sources. **Meetings** and **Agent sessions** are shortcut filters over the shared collection. Coding is a session type within Agent sessions.

The user also requires easy **source-first browsing**: identify what was captured through any connector, inspect its material and derived facts, decisions, actions, and observations, then follow its connected nodes. A shared node can have supporting evidence from several connectors; selecting one connector reveals its contribution without redefining the node as belonging only to it. Connector/platform origin, account/context, and content type are distinct dimensions.

Project detail leads with a sourced current understanding, then its history, unresolved disagreements, and traversable connections. Exact passages open inline, with a return path to the previous source and browsing state. Queries use the existing **Chats/Ask** flow with visible inherited scope and an option to broaden it. Connector exploration concerns captured contributions; it does not imply universal remote search.

The user plans to use **Composio for hundreds of connectors** and wants new origins to fit the same friendly Library experience. Browsing must scale through searchable origin selection rather than a destination or visible chip for every integration. The original service remains the visible origin; Composio is an integration transport.

## Capabilities and Constraints

- **Design approved — 2026-10-11:** after reviewing the existing-data simulation, the user explicitly approved this Library design baseline. Approval covers the interaction design described above: All sources and prominent work, scalable origin/account/container/content-type scopes, Sources/Outcomes/Nodes, inline evidence and project history, node traversal, View all nodes and unified search, scoped existing Chats, and Coding within Agent sessions.
- **Merged foundation:** PR #1210 is merged. This establishes backend groundwork; it does not establish completeness or correctness of historical data or delivery of the target experience.
- **Target experience:** an evolution of Library serving both confirmed jobs. Existing browsing and item details provide the starting point; source evidence, project history, and meaningful graph connections enrich that context.
- **Approval boundary:** production implementation and deployment require separate work. The approved preview does not establish that production records, history, ranking, or behavior are correct.
- **Source browsing:** All sources is the natural entry for origin-based exploration. Its origin choices and counts represent captured material the user can access, independently of content kind. The available integration catalog and connection status do not establish that material has been captured. Retain search, selected-source detail, precise evidence, and a return path across related items.
- **Connector scale:** metadata should drive display and discovery; capture, historical import, and ongoing updates require verified capabilities for each integration. Composio adoption is planned. This design does not implement it or establish universal ingestion coverage.
- **Evidence continuity:** future connector expansion must preserve captured source identities, versions, and citation paths while adding explicit service, connection, and content-kind metadata.
- **Terminology:** an atom is a meaningful unit of knowledge; a canonical node represents the thing it concerns; a source passage is its inspectable evidence. User-facing labels should lead with recognizable content.
- **Remaining implementation work:** production ranking, metadata enrichment, semantic-atom integration, evidence APIs, access-aware search and scope contracts, and richer version/change presentation. Existing graph size and coral counts do not establish the agreed prominence measure.

## Brand Commitments

Preserve CoralSwarm's established identity. The current authority is `../docs/design/DESIGN_SYSTEM.md`, supported by `app/globals.css` and the July 2026 redesign handoff it references. Earlier cream-era prototypes are historical references.

## Evidence on Hand

- Confirmed user choices: design only; clickable wireframe first; both jobs have equal priority.
- Latest direction: preserve the familiar Library experience and enhance it. Previous standalone explorer concepts were rejected.
- Accepted navigation: All sources is the default; recent sources and prominent projects/work topics coexist; View all nodes replaces the top-level Topics tab; Meetings and Agent sessions are shortcuts. Coding remains an Agent session type.
- Accepted search and query direction: distinguish nodes and sources in one Library search; connector scope exposes captured contributions; reuse Chats/Ask with visible inherited scope and the ability to broaden it.
- Confirmed source-first requirement: easily inspect material and outcomes from any origin. Named platforms and material types are examples, not an exhaustive list.
- Planned Composio adoption and hundreds of future connectors require scalable discovery. No production Composio integration or dynamic registry is delivered by this design task.
- Approved atom, canonical-node, source-evidence, and temporal-graph model.
- Existing application and current design-system implementation.
- PR #1210 merged as backend foundation; no historical-data validation is asserted here.
- The approved prototype supports a synthetic fixture and a local existing-data preview. The latter used a partial UI-read snapshot of 50 entries, 19 captured-text excerpts, and 51 nodes, plus 9 separately labeled `design_simulation` / `not_saved` outcomes. Private snapshots remain outside git. This supported design evaluation; it did not validate full coverage, original-passage completeness, or production data correctness.

## Product Principles

1. Evolve familiar Library browsing while giving session outcomes and project history equal importance.
2. Keep evidence reachable from the knowledge it supports.
3. Preserve context while moving among related knowledge, sources, and time.
4. Lead with familiar projects, meetings, and sessions; reveal technical identifiers and version details when needed.
5. Distinguish source statements, extracted knowledge, and changes over time without asserting unsupported conclusions.
