# CoralSwarm MCP Apps / ChatGPT inline widgets

The backend serves these four designed widget bundles through the standard
MCP Apps bridge, and retains `result-card-v1.html` for cached older
conversations. Their host transport and citation contracts are preserved.

Four self-contained HTML widgets for the four read tools MCP callers use
most: `ask_ocean`, `search_atoms`, `get_reef`, `recent_activity`. Built with
Preact + TypeScript, bundled by Vite (`vite-plugin-singlefile`) into one
HTML file per template with all JS/CSS inlined — the only external resource
is Google Fonts CSS plus font binaries (the widget sandbox allows
`fonts.googleapis.com` and `fonts.gstatic.com`).

This package is **not** part of the pnpm workspace (`pnpm-workspace.yaml`
deliberately does not list it) — it's a standalone npm-managed package so
its Preact toolchain never has to reconcile with the rest of the monorepo's
Next.js/React dependency graph. Use `npm`, not `pnpm`, inside `widgets/`.

## Layout

```
widgets/
  src/
    types.ts            wire shapes mirrored from backend structuredContent
    theme.ts             design tokens (light + dark), source-type colors
    openai.ts             shared MCP Apps/OpenAI bridge + no-host fallback
    mcpTransport.ts       validated parent-window transport for the SDK
    format.ts, citation.ts, answerParse.ts, eventDisplay.ts, links.ts
    components/           Card, Mandala, CiteChip, SourceCard, SourceExpanded,
                           Working, Empty, ReefPills, Icons
    templates/             AskOcean, SearchAtoms, GetReef, RecentActivity
    entries/               one .html + .tsx per template (Vite build inputs)
    test/                  fakeOpenAi mock + *.test.tsx
  fixtures/               *.json structuredContent fixtures (see below)
  dev/                    dev harness (index.html + main.tsx)
  scripts/
    build.mjs             drives 4 current + 1 legacy singlefile builds (see vite.config.ts)
    check-drift.sh         rebuild + diff against backend/assets/widgets/
  vite.config.ts          production build (singlefile, one entry at a time)
  vite.dev.config.ts      dev-server config for the harness
  vitest.config.ts
```

## Scripts

```
npm install
npm run build     # -> four current templates + result-card-v1.html in dist/
npm run check     # tsc --noEmit
npm test          # vitest run (vitest does NOT typecheck — always run both)
npm run dev        # dev harness, http://localhost:5173/?template=...
npm run drift      # rebuild + diff against backend/assets/widgets/*.html
```

`npm run build` is a small Node driver (`scripts/build.mjs`), not a plain
`vite build` — see the comment in `vite.config.ts` for why: under Vite 8
(Rolldown), `vite-plugin-singlefile` sets `output.codeSplitting = false`,
which Rollup/Rolldown only allows for a single input, so each template is
built with its own `vite build` invocation (`WIDGET_ENTRY=<name>`) rather
than one multi-input config.

**Determinism.** The five outputs must be byte-identical across repeated
builds from the same source (no hashes, no timestamps) — `check-drift.sh`
depends on it. Verified by building twice and diffing:

```
npm run build && cp -r dist /tmp/a && npm run build && diff -rq /tmp/a dist
# -> no output
```

## Shared presentation

`src/answerUi.ts` adapts the dependency-free `packages/answer-ui` source renderer
and scoped CSS to Preact and the existing light/dark host tokens. The same
Progress, EvidenceCard/List/Detail and CardFrame DOM/CSS is used by the web app's
React adapter. The widgets retain their own bridge, polling, navigation,
safe areas and responsive height budgets; React is not bundled here.

All four current tools share the frame and truthful loading presentation.
Ask answers and search results use the shared evidence cards; ask source
selection uses the shared full detail, and reef timeline cards retain their
complete supplied text. Recent activity rows remain activity, without evidence
labels. The legacy entry preserves old cached-result transport and reuses these
templates for recognized shapes, with a safe JSON fallback for other values.

## Runtime contract (OpenAI Apps SDK / MCP Apps)

The bridge uses the official `@modelcontextprotocol/ext-apps` 2.0.0 SDK,
reviewed against the [App API](https://apps.extensions.modelcontextprotocol.io/api/classes/app.App.html)
and [transport source](https://github.com/modelcontextprotocol/ext-apps/blob/main/src/message-transport.ts)
on 2026-09-15. The SDK owns initialization, JSON-RPC correlation/timeouts,
notification validation, and server-tool calls; our narrow `ParentTransport`
validates the parent window, then pins the first valid message's origin.
An opaque sandbox parent may use origin `null`; only the initial handshake
and opaque-origin destinations require a wildcard postMessage target.

Each mounted widget shares one connection across its hooks. All handlers are
registered before `ui/initialize`; the SDK completes the handshake with
`ui/notifications/initialized`. `ui/initialize` declares
`appCapabilities.availableDisplayModes: ["inline", "fullscreen"]` so a host
keeps its expand/fullscreen chrome — omitting that list is how Claude and
ChatGPT collapse the control on some surfaces. The bridge then:

- Receives arguments from `ui/notifications/tool-input` and partial-input
  notifications, clears a previous invocation, and invalidates old tool calls.
- Receives `structuredContent` from `ui/notifications/tool-result`, including
  results delivered immediately at initialization or later. Standard results
  take precedence over delayed `openai:set_globals` echoes.
- Maps initialization context and `ui/notifications/host-context-changed`
  into the light/dark, display mode, safe-area, locale, height, width,
  platform and device-capability hooks (see **Layout** below). Approved
  fonts/colors remain controlled by the widget design tokens.
- Reports intrinsic size using the SDK's ResizeObserver support, including
  citation expansion and fullscreen transitions.
- Sends `tools/call` for **only** `get_atom` (a citation marked
  `body_omitted: true`) and `get_task_result` (legacy working envelopes).
  Tool responses are unwrapped from `structuredContent`; host errors are
  surfaced in the existing card/empty-state treatment.
- Sends `ui/request-display-mode` for fullscreen and `ui/open-link` for deep
  links. No external network request is made by interactive tool calls.
- Closes the transport and observers at unmount/pagehide. Data is scoped to
  the current widget instance and never cached across users.

**Compatibility.** `window.openai` globals and methods remain supported for
legacy hosts and the original dev harness. Legacy context fills only fields
not supplied by a connected standard host. Outside a host, an inline
`<script type="application/json" id="cs-data">` payload or parent-origin
validated `postMessage({type:"cs:tool-output", toolInput, toolOutput})` can
supply preview data; tool calls resolve to `null` when no host is available.
A top-level preview never initiates a self-addressed handshake.

### Resource registration

The remote Streamable HTTP MCP server serves `text/html;profile=mcp-app`
from the generated files via `include_str!`:

| Tool | Versioned resource URI | Generated file |
|---|---|---|
| `ask_ocean`, `get_task_result` | `ui://coralswarm/ask-ocean-v2.html` | `ask_ocean.html` |
| `search_atoms` | `ui://coralswarm/search-atoms-v2.html` | `search_atoms.html` |
| `get_reef` | `ui://coralswarm/reef-overview-v2.html` | `get_reef.html` |
| `recent_activity` | `ui://coralswarm/recent-activity-v2.html` | `recent_activity.html` |

Tools advertise both `_meta.ui.resourceUri` and the compatible
`_meta["openai/outputTemplate"]`. Widget resource CSP permits Google Fonts
CSS and font files; the connect-domain allowlist is empty. `get_atom`
carries no output template; it and `get_task_result` remain the only tools
callable by an app.

`resources/read`, `resources/list`, and `resources/templates/list` retain
`ttlMs: 0` and `cacheScope: "private"`, the HTTP freshness fix from #951.
The fixed versioned URIs replace the temporary shared canary renderer, so
cached canary HTML cannot be reused for the designed widgets.

### Two stale canvas annotations (founder override — implemented per the override, not per the canvas)

1. `note-chip` ("Chip click = `callTool(get_atom)`") is **stale**. Chip
   expand renders INLINE from the `citation_details` entry the tool already
   returned (`title`/`content`/`source_type`/`author_name`/`channel_name`/
   `timestamp`/`age_days`/`reefs`). `get_atom` is called only when that
   citation carries `body_omitted: true` (see `SourceExpanded.tsx`).
2. `note-async` ("template attaches to get_task_result; Working covers the
   poll") is **stale** for the sync-backend future, still correct for
   today: `ask_ocean` returns the full completed answer directly once the
   backend goes synchronous for non-task clients; `AskOceanTemplate`
   handles BOTH: `toolOutput` already completed (render immediately), or the
   legacy `{task_id, status:"working"}` envelope (render `Working`, poll
   `get_task_result` every ~2s via `callTool`).

## Deep links

Resolved by reading the REAL `web/app/(app)/**/page.tsx` route handlers
(`useSearchParams()`), not guessed:

- `ask_ocean` → `/chats?q=<question>` (prefills the composer, same mechanism
  Today's "Ask about this day" button uses; there is no saved-conversation
  id an `ask_ocean` MCP call can link to).
- `search_atoms` → `/library/sources` (no query-prefill support exists on
  that route today — see "Backend fields needed").
- `get_reef` → `/topics?id=<reef_id>` (`/reef` is a dead route — client-side
  redirect to `/topics`, per `web/components/RouteRedirect.tsx` — linked
  straight to the real destination instead).
- `recent_activity` → `/today` (matches the artboard's own "open today"
  copy).
- A cite chip's "source" link / a search result row → `/coral?id=<atom_id>`.

All under `https://app.coralswarm.example` (confirmed via `web/next.config.ts`'s
own canonical-host redirect).

## Fixtures

One realistic `{toolInput, toolOutput}` fixture per template plus `empty`
(and `working`, ask_ocean only — the only template with a task-polling
state) variants, matching the artboards' rate-limiting example:

- `ask_ocean.synthesized.json` / `.empty.json` / `.working.json`
- `search_atoms.default.json` / `.empty.json`
- `get_reef.default.json` / `.empty.json`
- `recent_activity.default.json` / `.empty.json`
- `get_atom.body_omitted.json` — a raw `AtomDetail` (a `callTool` response,
  not a page-level `toolOutput`), used by the dev harness and tests to
  answer `get_atom` for `ask_ocean.synthesized.json`'s one
  `body_omitted: true` citation (chip 5).

## Dev harness

```
npm run dev
# -> http://localhost:5173/?template=ask_ocean&fixture=synthesized&theme=light&displayMode=inline
```

Query params:

| Param         | Values                                              | Default        |
|---------------|------------------------------------------------------|----------------|
| `template`    | `ask_ocean` \| `search_atoms` \| `get_reef` \| `recent_activity` | — (shows an index page listing every combination) |
| `fixture`     | one of that template's fixture variants (see above)   | template's first realistic fixture |
| `theme`       | `light` \| `dark`                                     | `light`        |
| `displayMode` | `inline` \| `pip` \| `fullscreen`                     | `inline`       |
| `maxHeight`   | any number (px)                                       | unset          |
| `platform`    | `mobile` \| `tablet` \| `desktop`                      | unset (browser fallback) |
| `touch`       | `true` \| `false`                                     | implied by `platform` |

The harness installs a real, spyable `window.openai` (`src/test/fakeOpenAi.ts`)
seeded from the chosen fixture; `get_task_result` resolves the `working`
ask_ocean fixture into the `synthesized` one after the first poll, and
`get_atom` answers only for the one `body_omitted` fixture atom — so the
harness demonstrates every state transition live, not just static renders.

### Standard MCP Apps production preview

After `npm run build`, open
`http://localhost:5173/mcp.html?template=ask_ocean&fixture=synthesized&delay=200`.
This separate harness loads the **compiled production HTML** in an opaque
sandbox iframe under a strict CSP without `unsafe-eval`, injects no
`window.openai`, and exchanges actual standard protocol messages.

Visible controls change template, fixture, theme, display mode, iframe
width (a numeric field plus 390/430/768/1024/1280 presets), the height the
host *reports* (to reproduce the clipping bug below), platform,
touch/hover capabilities and a bottom safe-area inset; send a
delayed/repeated result; cancel the tool; and reload the iframe. Every
control has a same-named URL param. In `fullscreen` the harness holds the
iframe at a fixed 800px box and ignores `size-changed`, because that is
what a real host does with a fullscreen widget — only an inline widget's
frame follows its reported intrinsic height. A protocol log exposes initialization, tool calls,
and link/display requests. The size counter records intrinsic-size messages.
The working fixture resolves via `get_task_result`, and citation 5 expands
through `get_atom`. The original harness remains available for comparison.

## Layout

Every template reads one hook — `useHostLayout()` in `src/layout.ts` — and
no template hardcodes a breakpoint. It resolves, in precedence order:

1. the standard MCP Apps host context (`containerDimensions.width|maxWidth`,
   `containerDimensions.maxHeight`, `displayMode`, `platform`,
   `deviceCapabilities.{touch,hover}`, `safeAreaInsets`),
2. the legacy `window.openai` surface (`maxHeight`, `displayMode`,
   `safeArea.insets`, and `userAgent.device.type` / `userAgent.capabilities`,
   translated by `deriveLegacyLayout` — `tablet` folds into `mobile` and
   always implies touch),
3. live browser signals when both are silent — `window.innerWidth` (kept
   current by a `resize` listener), `(pointer: coarse)` and `(hover: hover)`.

A field the standard host has ever supplied is never overridden by a later
legacy echo. Width is the one subtle case: the host's snapshot wins until
this window observes its *own* `resize`, after which the live value stays
authoritative (a `resize` is a direct fact about the iframe's current box,
where a host message can be stale).

Media queries are not the mechanism — they cannot see `platform` or
`displayMode`, and the container is an iframe the host sizes independently
of the device. `matchMedia` appears only as the fallback source above.

**Size tiers** (from width): `compact` < 480, `regular` 480–899, `wide`
>= 900. ChatGPT's inline container is 768 (`regular`); a phone is
`compact`; a desktop fullscreen shell is `wide`.

| Tier | What changes |
|---|---|
| `compact` | 12px gutters; `recent_activity` drops the 40px time gutter and shows the time inline; `search_atoms` clamps the snippet to two lines instead of one ellipsized line, and wraps the age tag under the title; `get_reef` stacks the timeline date above its card, drops the title to 21px, and shows one relationship pill per row; `ask_ocean` renders one source column and a 16/18/20px fullscreen pad |
| `regular` | today's 14–18px gutters; two source columns; `ask_ocean`'s fullscreen sources rail stacks *below* the answer (there is no room for a 340px rail beside a readable measure) |
| `wide` | the artboards' original layout — 340px sources rail beside the answer, three columns when a source is expanded, 28/36/32px pad |

**Heights.** An inline widget never sets its own height: the SDK's
ResizeObserver reports intrinsic size and the host resizes the iframe. Two
consequences the bridge encodes:

- A host's `containerDimensions.height` describes the iframe *as it is now*,
  not a cap. Treating it as `maxHeight` pins the measured
  `documentElement` (`setupSizeChangedNotifications` measures at
  `height: max-content`) to the current height, so the observer reports no
  growth, the host never enlarges the frame, and content is clipped. Only
  `maxHeight` becomes a cap; a `height` clears it.
- `ask_ocean`'s **fullscreen** shell is the one place a real bound is
  needed, because its answer and sources panes each scroll. `height: 100%`
  resolves against an auto-height body and collapses, so it uses
  `VIEWPORT_HEIGHT` (`100dvh`, falling back to `100vh` where
  `CSS.supports` says no) with `box-sizing: border-box` and the safe-area
  insets as padding.

`recent_activity` previews a subset of events behind a
"Show all N events" toggle. The count is `total` in fullscreen; otherwise,
when the host gives a `maxHeight`, `floor((maxHeight - 120) / 72)` clamped
to `[3, 10]` (120px of fixed chrome, 72px per event row); otherwise the
tier default — 3 compact, 5 regular, 8 wide.

**Touch** (`deviceCapabilities.touch`, or `pointer: coarse`) raises every
tappable target to a 44px minimum. Non-zero **safe-area insets** become
padding on the outermost rendered node.

## Backend fields needed

Recorded here rather than invented in the widget (per the task brief).
None of these block shipping — every template renders sensibly without
them, per the notes in the code:

1. **No latency/timing field.** Neither `ask_ocean`'s completed body nor
   `get_task_result`'s does — the artboards' "· 2.3s" header suffix isn't
   renderable. (`templates/AskOcean.tsx`'s `sourcesMeta`.)
2. **No per-citation `ocean_id`.** `citation_details[i]` carries no ocean
   id, so ask_ocean's header/sources-divider can show a coral count but
   never a real "N oceans" count (the artboards show one). `search_atoms`
   results DO carry `ocean_id`, so that template's ocean count is real, but
   only over the RETURNED page, not a true total-oceans-searched figure.
3. **No total-match count distinct from the returned page.**
   `search_atoms` returns only what fits `limit` — no total-matches or
   oceans-searched aggregate, so "12 corals match / 6 of 12 shown" (the
   artboard) can only ever show "N of N shown" where N = what was returned.
4. **No "settled into" reef id/type on a citation.** `citation_details[i].reefs`
   / `search_atoms` results' `reefs` are plain title strings — no reef id,
   no reef type. The expanded source card's "settled into" section
   therefore renders plain tags, not the typed `◆ decision`/`thread` pills
   the artboard shows, and can't deep-link to `/topics?id=`.
5. **No ocean display name on `get_reef`.** Only `ocean_id` — the artboard's
   "platform ocean" header segment isn't renderable; the header instead
   shows `{type} reef · {atom_count} corals`.
6. **`get_reef.relationships` carries generic entity/topical predicates**
   (`works_on`, `related_to`, `manages`, …; see
   `backend/src/relationships/predicates.rs`), not a dedicated
   decision-supersession or "related topic" edge type. The artboard's fixed
   "supersedes"/"related" vocabulary doesn't exist — the widget renders
   whatever predicate the ocean actually recorded, humanized
   (`_` → space), with a best-effort string match against `node.title` to
   decide which side is "this reef" (get_reef's `relationships[]` carries
   `subject`/`predicate`/`object` as title strings, not the two node ids —
   `AnswerRelationship.{src,dst}_node_id` exist internally but are dropped
   before this tool's `json!({...})`).
7. **`recent_activity`'s real event vocabulary is much narrower than the
   artboard's.** The only four `insert_activity_event` call sites in the
   whole backend today are `atom_ingested`, `query_run`,
   `supersession_created`, `promote` (`rg -n "insert_activity_event\("`)
   — no `decision`/`session`/`meeting`-transcribed/conflict-with-titles
   events exist yet, and none of the four carry an actor DISPLAY NAME
   (`user_id` only, never resolved to a name) or a human summary beyond
   their raw payload fields. `src/eventDisplay.ts` maps the four real types
   to a badge + best-effort body text and falls back generically for
   anything else; day/people counts are computed CLIENT-SIDE from the flat
   paginated `events[]` list (no backend day-bucketed aggregate).
8. **`/library/sources` has no query-prefill.** `search_atoms`'s "open
   library" link can't carry the search query through (`/coral`, `/topics`,
   and `/chats?q=` all support the query params they need; `/library/sources`
   reads none).

## Deviations from an artboard (and why)

- **Fullscreen "expand" affordance.** The artboards treat Fullscreen as a
  mode the HOST switches into, not a button the component draws. A small
  quiet "expand" text link sits in the ask_ocean card footer so Fullscreen
  is reachable when a host has no chrome of its own, and so a host that
  sizes the iframe to the card (Claude) cannot clip a sibling below it.
  The answer/sources scroll under a `maxHeight` cap; the footer does not.
- **"Settled into" pills / relationship pills / ocean names** — see
  "Backend fields needed" #4-6 above; each renders a strictly honest
  degrade of the artboard rather than fabricated type/name data.
- **`search_atoms` filter chips are computed over the returned page only**
  (see #3) — clicking one filters client-side; it never re-queries.
- **`recent_activity` badges/copy** — see #7; every string is derived from
  a REAL payload field, never invented meeting/session titles.
