# Design brief: Connectors redesign

Brief for Claude Design. Claude Design produces the visual direction and
every screen state for the redesigned connectors experience in the web app.
No Figma: the Claude Design output is what engineering builds from.

## Problem

The connectors page has four unrelated connect flows today:

- **Slack**: a custom card, an onboarding sheet and checklist, a backfill
  offer, and its own channel page.
- **Microsoft and GitHub**: a generic tile with an admin-only gate and a Sync
  button. GitHub then sends you to a separate repo page.
- **Google Workspace**: its own component for Gmail, Calendar, Meet and Chat,
  plus a separate Chat spaces page.
- **Composio toolkits** (Jira, Linear, and others): a strip of text links
  above the grid that lead to a separate capture page.

Every connector looks different and asks the user different questions in a
different order. The redesign gives every connector one lifecycle and one set
of components.

## The model to design for

**Connector.** One connector is one provider sign-in. It gets one card in the
grid. Inside a connector is a list of **sources**:

| Connector | Sources |
|---|---|
| Slack | Channels |
| Microsoft | Outlook mail, Calendar, Teams chats, Teams channels, Teams meetings |
| Google | Gmail, Calendar, Meet, Chat |
| GitHub | Repositories |
| Jira, Linear, Confluence, HubSpot, Zendesk, Dart | One source each (workspace-wide capture) |

**Lifecycle.** Every connector goes through the same steps:

1. **Connect.** The user signs in with the provider. Every connection is
   personal: there is no admin or org-wide install in this design.
2. **Choose sources.** The user turns on the sources they want. A source that
   needs org setup the user can't do alone is listed but locked, with the
   label "Requires your IT admin" and one sentence explaining why (for
   example, Teams channel messages need tenant admin consent).
3. **Choose scope.** For each source, one picker component:
   - A mode switch: **Only what I pick** (the default) or **Everything I can see**.
   - Under "Only what I pick", a searchable list of items (channels, repos,
     spaces, threads), starting with nothing selected.
   - Sources with no item list (the Composio toolkits) show only
     "Everything I can see".
4. **Import history.** One question for every source: *Last 7 days · Last
   30 days · Up to 90 days · None*. 90 days is the hard limit everywhere.
   Sources that can't import history (live meeting capture) skip this step
   and say why in one line.
5. **Manage.** This is the ongoing state, on the detail page.

**Disconnect** stops new capture and revokes the sign-in. Everything already
captured stays. A single source can also be paused without disconnecting.

## Screens to design

### 1. Connectors page (`/connectors`)

- `PageHead` titled "Connectors", with a one-line lead.
- **Connector grid.** All connectors in one grid using the same card. The
  Composio toolkits are not set apart. Their only difference is a `Beta`
  badge. Never show the word "Composio".
- **"Use CoralSwarm in other tools" section** below the grid (this moves here
  from `/settings/connections`):
  - The MCP address `https://api.coralswarm.example/mcp` in mono, with a copy
    button.
  - A client picker (`Seg` or `Tabs`): Claude, Claude Code, ChatGPT, Cursor,
    Codex. Selecting one shows that client's setup steps.
  - One line saying there is no key to copy: the user signs in with their
    CoralSwarm account.
- Removed from today's page: the desktop download tile, the "other ways in"
  links, the Zoom coming-soon tile, the standalone Gmail add-on and followed
  threads sections. The last two move into the Gmail and Outlook source
  detail.

### 2. Connector card

The same component for every connector. It contains:

- `ProviderMark` (provider glyph on its hue) and the provider name.
- A **status pill** from one shared set:
  `Not connected` · `Setting up` · `Active` · `Importing history` ·
  `Needs attention` · `Reconnect needed`.
- A **freshness line** in mono: "4 of 5 sources · last captured 3 min ago".
  When the status is unhealthy, the backend's plain-language message replaces
  it (for example "Slack revoked access on Oct 27. Reconnect to resume.").
- **One primary action** that matches the status: `Connect` ·
  `Continue setup` · `Manage` · `Reconnect`.

Design every status on at least Slack (single source) and Microsoft (multiple
sources, two of them locked as "Requires your IT admin").

### 3. Connect wizard (modal)

A `Modal` that runs the first-time lifecycle: Connect → Choose sources →
Choose scope → Import history → Done. Design:

- The step indicator, and how it shrinks for single-source connectors
  (Slack, GitHub, Jira) where "Choose sources" is skipped.
- The **sign-in hand-off**: the user leaves for the provider and comes back.
  Show a "Waiting for you to finish signing in to Microsoft…" state and an
  "Sign-in expired, try again" state.
- **Choose sources** for Microsoft, including the locked "Requires your IT
  admin" rows.
- **Choose scope** with:
  - a long list (Slack, 200 channels: search, select all visible, a selected
    count);
  - an empty list ("You aren't in any spaces yet");
  - "Everything I can see" selected, with a one-line note about what that
    includes.
- **Import history** with the four options. Also design the skipped variant
  for Teams meetings.
- **Done**: a summary of what will be captured and what history is
  importing, with a link to the detail page.
- The user can close the wizard at any point. The card then reads
  `Setting up` with `Continue setup`.

### 4. Connector detail page (`/connectors/[provider]`)

The Manage view. The same template for every connector:

- A header with the mark, name, status pill, and the account it's signed in as.
- **Sources list.** Each row has:
  - its own status;
  - a pause toggle;
  - a scope summary ("12 channels" / "Everything I can see");
  - history import progress when one is running (`Meter`);
  - last captured.
- Opening a source shows the same scope picker as the wizard, inline, plus
  any source-specific settings. Gmail and Outlook carry followed threads and
  the Gmail add-on token here.
- A "Requires your IT admin" source row in its locked state.
- **Disconnect**, with one confirm dialog for every provider: "Stop capturing
  from Slack? What's already captured stays in your ocean."
- Unhealthy states: a `Notice` at the top with the backend message and
  `Reconnect`.

### 5. Onboarding step (`/onboarding/connectors`)

The same cards and the same wizard inside the onboarding shell. No separate
UI. Show it with Slack highlighted as the recommended first connector.

## Visual rules

Use the CoralSwarm design system as it exists. Don't introduce new values. If
something is missing, flag it as a proposed token rather than hardcoding it.

- **Tokens**: `web/tailwind.config.ts`.
  - Surfaces: `bg`, `bg-2`, `bg-3`.
  - Ink: `ink`, `ink-2`, `ink-3`, `ink-4`.
  - Lines: `line`, `line-2`, `line-3`.
  - Brand: `brand` #F77764, `brand-2`, `brand-3`.
  - Status: `ok`, `warn`, `bad`.
  - Hues: `coral`, `pink`, `amber`, `teal`, `plum`.
  - Radii: `card` 12, `control` 10, `field` 9, `row` 8, `badge` 6, `pill`.
  - Shadows: `card`, `pop`, `frame`.
- **Type voices** (`docs/design/DESIGN_SYSTEM.md`):
  - `font-display` (Instrument Serif) for page and wizard titles, the human
    voice, with one italic brand word at most.
  - `font-sans` (Geist) for UI and body.
  - `font-mono` (JetBrains Mono) for counts, timestamps, freshness and
    status labels.
- **Components**: build from the kit in `web/components/ui/`:
  - `Card`, `CardHead`, `Badge`, `Chip`, `Verdict`, `Button`, `Toggle`,
    `Seg`, `Tabs`, `SearchInput`;
  - `ListRow`, `RowTitle`, `RowMeta`, `Modal`, `Notice`, `Meter`, `Empty`,
    `Loading`, `ErrorNote`, `PageHead`.

  Name the component each element maps to, so the build has no guesswork.
- **Provider hues** (from `providerMeta.ts`): Slack amber, Microsoft plum,
  GitHub pink, Gmail/Google coral. Assign hues to the Composio toolkits from
  the same five and note the choices.
- Status pills use the `Verdict` treatment (the word carries meaning, and the
  tone colors only the glyph and border), so they stay readable without color.

## Copy rules

- Plain language. Name the provider and the thing ("channels", "repos"),
  never internal terms (grant, adapter, toolkit, Composio, ingest).
- Say what is captured and what isn't (for example, GitHub: "Pull requests,
  reviews, issues, releases and commits. Never your source code.").
- Every non-active state says what happened and the one thing to do next.

## Deliverables

1. **Two or three directions** for the connectors page and card, side by
   side, before going deep. Pick one, then continue.
2. For the chosen direction, every screen above in every listed state, at
   desktop width (1280) and a narrow width (390).
3. A short component map: each new or changed element → the `ui/` component
   it uses, plus any proposed new token.

## Out of scope

- Org-wide or admin installs (Slack bot, Microsoft admin consent, GitHub org
  install, Google Chat app).
- The desktop app.
- Zoom and any connector not listed above.
- Changes to what each provider captures. This is a UX and contract
  redesign, not a capture change.
