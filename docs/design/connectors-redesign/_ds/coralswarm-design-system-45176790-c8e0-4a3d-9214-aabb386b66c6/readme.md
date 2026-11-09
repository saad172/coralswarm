# CoralSwarm — design system

CoralSwarm is a team knowledge product: *"your team's knowledge ocean — built
from the reef up."* You hand it material — a pasted note, a meeting transcript,
a Slack channel, an AI coding session — and it splits, indexes and groups that
material into topics. You ask questions; every answer cites the source it came
from, and when two sources disagree it says so instead of picking one.

Its vocabulary is marine and consistent: a **coral** is one captured atom of
material, a **reef** is a topic formed from several corals, an **ocean** /
**workspace** is a boundary that content does not cross unless someone promotes
it, and the **swarm** is the system doing the work.

## Sources this system was built from

- **`web/` — the Next.js 15 web + desktop client** (attached to this project as
  a read-only local folder). Package `@coralswarm/web`. Tailwind 3.4 with a
  project theme, Clerk auth, TanStack Query, React 18. This is the primary and
  only source of truth used here.
  - `web/app/globals.css` and `web/tailwind.config.ts` — the token definitions,
    ported into `tokens/*.css` verbatim.
  - `web/components/ui/*` — the internal UI kit, which is the component
    inventory recreated in `components/`.
  - `web/components/icons.tsx` — the icon set, ported into
    `components/icons/Icon.jsx`.
  - `web/app/(app)/**`, `web/app/onboarding/**`, `web/app/overlay/page.tsx` —
    the product screens, recreated in `ui_kits/`.
- The codebase repeatedly cites two internal documents that were **not**
  attached and could not be read: the *2026-07 redesign handoff*
  (`New UI Design/design_handoff_coralswarm_redesign/`) and
  `docs/design/DESIGN_SYSTEM.md`. Everything they define is quoted verbatim in
  the code comments this system was built from, but if you have them, read them.

There is **no logo file anywhere in the codebase**. The brand mark is drawn in
code — `components/ui/Mark.tsx` — so it has been ported as a component rather
than reconstructed as an image. See *Brand mark* below.

### Products / surfaces

| Surface | What it is | UI kit |
| --- | --- | --- |
| The app | The web client, and the same build inside a Tauri desktop shell | `ui_kits/app/` |
| First run | Personal onboarding: how it works → recovery key → sources | `ui_kits/onboarding/` |
| Live overlay | An always-on-top caption HUD during a recorded meeting | `ui_kits/overlay/` |

The 2026-07 redesign is the defining event in this codebase. Twelve rail
destinations collapsed to **five places plus two utilities**: Chats · Library ·
Today · Review · Workspaces (+ Connectors, Settings). Meetings and coding
sessions stopped being destinations and became *source types inside Library*.
The canvas flipped from warm cream to cool neutral by rewriting token values in
place — never by adding parallel names — which is why the coral brand and the
five hues survived untouched.

---

## Content fundamentals

**The house voice is plain, direct, and structurally honest.** It is the voice
of a system explaining what it did and what it cannot do. It never sells.

**Person.** Second person for the user, third for the system. "You hand it
things." "It forms topics." "Nothing happens that you didn't start." The
product refers to itself as "it", almost never "we" — "we" appears only where a
human promise is being made: *"We can't read it — which also means we can't
recover it for you."*

**Casing.** Sentence case everywhere. Title Case appears nowhere. Mono eyebrows
and chips are written lowercase in the source and uppercased by CSS
(`your workstream`, `timeline`, `candidate queue`). Kind labels are lowercase
nouns: `decision`, `person`, `thread`, `reference`.

**Punctuation.** Em dashes carry the second clause — that is the house
construction. Ellipses mark in-progress states (`thinking…`, `Stopping…`,
`surfacing the library…`, `Search topics…`). Middots separate metadata:
`SLACK · 12 MAR`, `answered from 4 sources · 2.3s`.

**Honesty over polish.** The most characteristic copy admits a limit:

- *"Nothing in this scope clears the bar for a confident answer."*
- *"The connection dropped before this finished."*
- *"Focus-block capture runs on the desktop app and isn't wired into this build."*
- *"We can't read it — which also means we can't recover it for you, so the next screen matters."*
- *"Your mic is silent — check it isn't muted. The other side is still recording."*

Never "Something went wrong." Say what happened and offer the recovery.

**Consent language.** Anything that moves data says who will see it and that it
was the user's doing: *"Nothing leaves a private workspace unless you promote
it."* · *"proposed for your review — you confirm"* · *"nothing arrives on its
own"* · *"Nothing becomes a topic, a link, or a merge without you."*

**Titles are sentences.** Serif page titles read as claims, not labels:
*"Things the system thinks are true."* · *"Here's how it actually works"* ·
*"Ask anything"* · *"All clear — nothing pending review."*

**Empty states name the cause, not the absence.** *"Add a source or ask a
question and topics will start forming here."* Never "No data".

**Loading states are verbs, in mono, lowercase.** `surfacing the library…`,
`opening this chat…`, `the review queue…`, `reading today…`. Never a spinner.

**Numbers are exact and mono.** `3,140 sources`, `2.3s`, `82%`, `12 saved`.
Ages are coarse and abbreviated: `just now`, `3h`, `2w`, `1mo`.

**No emoji.** Not one, anywhere in the product. The only non-alphabetic glyphs
are functional: `⌘K` `⌘N` `▾` `⠿` (drag grip) `⚑` (mark this moment) `✓` `↓`
`·`. Everything else is a stroked SVG icon.

---

## Visual foundations

**Canvas.** Cool neutral, never warm. White (`--bg`) content surfaces sit on a
grey canvas (`--bg-2`, `#F5F6F8`); the sidebar shares the canvas grey so the
white content column reads as the object. `--bg-3` is the deeper backdrop
behind floating frames.

**Colour discipline.** Coral (`--brand`, `#F77764`) is an *accent*, not a
default — active tab underlines, dots, citation chips, meters, the new-chat
plus, and the single consequential action on a screen. **The primary solid
button is ink (`#14161A`), not coral.** Getting this wrong is the fastest way to
look off-brand. Destructive lives on `--bad`; the coral wash `--brand-soft` is a
*background* (citations, avatars, count pills, selected tiles) and must never
carry a destructive meaning.

**The five hues are semantic.** coral = decisions and the brand · pink =
entities and people · amber = sources and extraction candidates · teal =
meetings and ingested content · plum = systems, references, archive. A hue is
never chosen for prettiness. Tints are `color-mix(in srgb, var(--hue-x) 18%,
transparent)` with the hue itself as the label colour.

**Type.** Three families, three jobs. **Instrument Serif** (400 only, italic
available) for page titles, answers-in-waiting and empty states — the one
editorial gesture. **Geist** for all UI and body. **JetBrains Mono** for
eyebrows, counts, ids, provenance and the entire dark overlay. Sizes carry half
pixels on purpose (12.5, 13.5, 15.5, 9.5) — copy them exactly; do not snap them
to a 4px grid. The house accent is **one word of a page title in coral italic**,
used at most once per screen.

**Backgrounds and imagery.** There are none. No photography, no illustration,
no gradients, no hero images, no stock. The only decorative surface in the
entire product is `.reef-bg`, a 24px radial-gradient dot grid in `--line`.
People are represented by initial avatars. If a design needs an image, that is a
signal it is drifting off-brand — ask before adding one.

**Borders and shadows.** 1px borders do the separating; shadow only lifts or
floats. Three border weights (`--line` on cards and fields, `--line-2` for
dividers, `--line-3` for in-card rules) and five shadow levels
(`row`, `card`, `pop`, `frame`, `overlay`) that go from "barely there" to
"floating over the desktop". No inner shadows anywhere.

**Corner radii** are named for what they wrap and top out at 14px: badge 6 ·
row 8 · field 9 · control 10 · card 12 · frame 14 · full 999. A card is white,
12px, 1px `--line`, `--shadow-card`, 14px padding. A quiet card is near-white
(`--bg-subtle`) with a `--line-2` border and no shadow.

**Selection anatomy — the one rule people break.** An active row is white +
1px `--line` + `--shadow-row` + weight 500. **Inactive rows carry a transparent
1px border** so nothing shifts by a pixel when selection moves. This is encoded
in `NavRow` and `FacetRow`; do not hand-roll it.

**Hover.** Backgrounds go to `--bg-hover`; text goes from `--ink-2` to `--ink`;
quiet borders go from `--line` to `--ink-3`. Never opacity fades, never scale.
**Press.** No transform, no shrink — the colour change is the feedback.
**Focus.** A 3px coral ring at 20–22% (`0 0 0 3px color-mix(in srgb, var(--brand)
20%, transparent)`) with the border turning coral. **Disabled.** `opacity: .5`
and `cursor: not-allowed`.

**Motion is almost absent.** A 0.15s colour/border/shadow transition is the
house default and is the *only* transition on most elements. Nothing translates,
scales, or slides. There are exactly two animations in the product: `cs-pulse`
(a coral ring on a "new since your last visit" dot) and the skeleton pulse.
No page transitions, no easing curves beyond `ease`.

**Transparency and blur.** Blur is never used. Transparency appears in three
places only: the modal scrim (`rgba(20,22,26,.28)`), `color-mix` hue tints, and
the dark overlay panel (`--ink` at 88%). Everything else is opaque.

**Layout rules.** The sidebar is fixed open at 224px — with five places there is
nothing to collapse, and a stable rail means the content column never reflows.
The header is 52px. Content is centred at max 1040px with 28/24 padding. A chat
thread is capped at 720px in Focus mode and 660px in Workbench (with the 368px
context drawer open); the composer pins to the bottom of the viewport, so its
menus open *upward*. Lists are borderless — a mono eyebrow header, then rows on
`--line-3` rules. Titles truncate; rows never wrap to two lines.

**The dark surface.** Exactly one exists: the live-caption overlay. Panel is
`--ink` at 88%, text is ivory `#f4eee6`, everything is mono, and its status
colours are the same `--warn` / `--bad` / `--brand` tokens.

---

## Iconography

**One set, drawn in-house.** `web/components/icons.tsx` defines ~38 stroked SVG
glyphs on a 24x24 grid: `fill: none`, `stroke: currentColor`,
`stroke-width: 1.8`, round caps and joins. They are rendered at 13px (inline
actions), 14px (sidebar utilities) and 15px (nav rows, buttons) — occasionally
18–19px in specimen contexts. There is **no icon font, no sprite sheet, no CDN
icon library, and no PNG icons** anywhere in the codebase.

All of them are ported verbatim into `components/icons/Icon.jsx` and shown in
that directory's card. **Do not substitute Lucide, Heroicons or Feather** — the
1.8 stroke on a 24px grid reads noticeably heavier than Lucide's 2/24 and
lighter than Heroicons' outline set, and several glyphs (`reefs`, `atlas`,
`currents`, `coral`) are marine metaphors with no equivalent anywhere.

**Emoji are never used.** Unicode is used functionally and sparingly: `⌘K`,
`⌘N`, `▾` on menu triggers, `⠿` as the overlay's drag grip, `⚑` for "mark this
moment", `✓` in a selection checkbox, `↓` in "show 20 more ↓". Provider logos
(Google, Apple, X on the auth screen) come from Clerk's CDN, not from this repo.

## Brand mark

There is no logo file. The mark is **generated in code**: five concentric rings
of dots at radii 0 / .14 / .25 / .35 / .45, ring counts 1 / 6 / 10 / 14 / 18
scaled by a `density` prop, drawn in a 0–1 viewBox and filled by cycling the
five hues. The wordmark is lowercase `coral` + the mandala + `swarm`, set in
Geist 600 at -0.045em tracking. Both are ported into `components/brand/`.

Densities in use: **.8** (sidebar account row, 22px), **.75** (answer
provenance mark, 18px; the wordmark's inline dot), **.6** (monochrome, on dark).
Never redraw or re-space it; use `<DotMandala>` / `<Wordmark>`.

### Intentional additions

- **`Icon`** — a name-keyed wrapper around the codebase's individual icon
  components, so a design can iterate glyph names. Geometry is unchanged.
- **Semantic token aliases** (`--text-body`, `--surface-card`, `--border-default`,
  `--focus-ring`, `--overlay-panel`, …) in `tokens/colors.css`. They resolve to
  the existing base tokens; the codebase itself uses the base names.
- **Layout constants** (`--sidebar-w`, `--header-h`, `--thread-max`, …) lifted
  out of the shell's inline Tailwind so a consumer can reproduce the geometry.

Nothing else was invented. Families the codebase does not define — Tooltip,
Toast, Accordion, DatePicker, breadcrumb, pagination — are deliberately absent.

---

## Index

### Root
- `styles.css` — the entry point. Link this one file. Imports only.
- `readme.md` — this file.
- `SKILL.md` — Agent Skills front-matter, for using this system in Claude Code.
- `thumbnail.html` — the homepage tile.

### `tokens/`
`fonts.css` (Google Fonts import + family names) · `colors.css` · `typography.css`
· `spacing.css` · `radii.css` · `shadows.css` · `motion.css` · `base.css`.

### `guidelines/` — foundation specimen cards
Colours (brand, five hues, hue tints, surfaces, ink, borders, semantic), Type
(display, coral-italic accent, sans scale, mono), Spacing (scale, layout
constants), Brand (radii, shadows, reef texture, motion).

### `components/` — 52 components in 11 groups

- **core/** — `Button`, `IconButton`, `SuggestionPill`, `Card`, `CardHead`,
  `Panel`, `Divider`
- **text/** — `Eyebrow`, `Mono`, `PageTitle`, `PageHead`, `SectionHead`
- **marks/** — `Badge`, `Chip`, `CountPill`, `Dot`, `ScopePill`, `Avatar`
- **forms/** — `Input`, `Textarea`, `Select`, `SearchInput`, `Toggle`
- **navigation/** — `Seg`, `Tabs`, `FacetRow`, `NavRow`
- **lists/** — `ListHead`, `ListHeadCell`, `ListRow`, `RowTitle`, `RowMeta`,
  `TableGrid`, `Th`, `Td`
- **feedback/** — `Empty`, `Loading`, `ErrorNote`, `Skeleton`, `Meter`,
  `StackedMeter`
- **overlays/** — `Modal`, `Popover`, `MenuItem`
- **citations/** — `CiteChip`, `SourceCard`, `AnswerMeta`
- **brand/** — `DotMandala`, `Wordmark`
- **icons/** — `Icon`

Each component ships `<Name>.jsx`, `<Name>.d.ts` (props contract) and
`<Name>.prompt.md` (what & when, plus a usage example). Each directory has one
`@dsCard` HTML showing its states.

### `ui_kits/`
- `app/` — the product: shell, Chats (Focus + Workbench), Library, Today,
  Review, Workspaces, Settings, Connectors.
- `onboarding/` — the personal first-run flow.
- `overlay/` — the dark live-caption HUD.

### Not included
- **Fonts** are loaded from Google Fonts, exactly as the codebase does through
  `next/font/google`. No binaries exist in the repo to copy.
- **No logo, illustration or photographic assets** exist in the source, so
  `assets/` is empty by design. Render the brand name in type, or use
  `<Wordmark>` / `<DotMandala>`, wherever a mark is needed.
- **Slides** — no deck or slide template was provided, so none were invented.
