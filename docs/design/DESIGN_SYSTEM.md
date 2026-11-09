# CoralSwarm Design System

Canonical reference for all product UI (web).

> **The visual source of truth is the 2026-07 redesign handoff**, in
> `New UI Design/design_handoff_coralswarm_redesign/` (README + `designs/*.dc.html`).
> It flips the canvas from warm cream to cool neutral and collapses the 12-item
> rail into five places. The token values below are the redesign's.
>
> `docs/design/coralswarm-prototype.html` and the other HTML files in this
> folder are the **superseded cream-era** prototypes. They are kept for
> reference only — do not take colour, radius or shadow values from them.

Implementation lives in:

- `web/tailwind.config.ts` — Tailwind tokens (semantic class names)
- `web/app/globals.css` — CSS variables + base styles + component classes
- `web/app/layout.tsx` — fonts via `next/font/google`
- `web/components/ui/` — the React UI kit (`index.ts` is the entry; `@/components/ui` resolves to it)
- `web/lib/motion.ts` — the `motion` package entry point and the motion constants

**The rule: all web UI must use these tokens and components. No
hardcoded colors, font stacks, shadows, or radii in screens.** If a screen
needs a color that isn't here, the design system changes first.

## Vocabulary

**slot** — a billing place for one member of an org; **seat** — access to a
team ocean, granted by routing. Never interchange (CS-433, D15).

## Tokens

### Surfaces & ink

| Token | Value | Tailwind | Use |
|---|---|---|---|
| `--bg` | `#FFFFFF` | `bg` (alias `surface`) | Content surface; cards sit on `--bg` with a `--line` border |
| `--bg-2` | `#F5F6F8` | `bg-2` (alias `canvas`) | Sidebar, page canvas, inset panels |
| `--bg-3` | `#EBECEF` | `bg-3` | Document backdrop behind frames |
| `--bg-hover` | `#EDEEF1` | `bg-hover` | Row hover, pressed field |
| `--bg-subtle` | `#FCFCFD` | `bg-subtle` | Alternating rows, quiet cards |
| `--bg-subtle-2` | `#FAFBFC` | `bg-subtle-2` | The same, one step deeper |
| `--ink` | `#14161A` | `ink` | Primary text, dark buttons |
| `--ink-2` | `#4B5058` | `ink-2` (alias `ink-muted`) | Secondary text, body copy |
| `--ink-3` | `#8A9099` | `ink-3` (alias `ink-faint`) | Muted labels, captions, mono eyebrows |
| `--ink-4` | `#A6ABB3` | `ink-4` | Faintest text, disabled glyphs |
| `--line` | `#E4E6EA` | `line` | Standard 1px border |
| `--line-2` | `#EDEEF1` | `line-2` | Internal dividers (header/section rules) |
| `--line-3` | `#F2F3F5` | `line-3` | Faintest divider inside cards |

Note `canvas` now aliases `--bg-2`, not `--bg`: in the redesign the page canvas
is the grey inset and white is the content surface, which is the reverse of the
cream system.

### Brand & accent

| Token | Value | Tailwind | Use |
|---|---|---|---|
| `--brand` | `#F77764` | `brand` (alias `coral`) | Coral accent: dots, active marks, meters, primary actions |
| `--brand-2` | `#D14A38` | `brand-2` (alias `coral-dark`) | Links, accent text on light, badge text |
| `--brand-soft` | `#FFE1D9` | `brand-soft` (alias `brand-3`) | Coral chip / avatar / citation background |
| `--accent` | `#2A9D8F` | `accent` | Secondary accent (teal) |

**`brand-3` is a light wash, not the destructive red.** The handoff's token
table proposes the name `brand-3` for destructive `#C24A3A`; that name was
already taken here by the soft coral fill behind `.cite`, `.cs-hl`, `Chip` and
the selected-tile states. Destructive stays on `--bad` (which is already
`#C24A3A`), and `brand-3` was retuned to `--brand-soft`'s value. Prefer
`brand-soft` in new code. Use `--brand-soft` **neat** for a selected/highlighted
fill — do not `color-mix()` it down over `--bg`, or it disappears against white.

### Hues

The five-hue family colors *kinds of knowledge*, not moods. Use them
consistently:

| Token | Value | Tailwind | Semantics |
|---|---|---|---|
| `--hue-coral` | `#F77764` | `hue-coral` | Decisions; the brand itself |
| `--hue-pink` | `#EB7290` | `hue-pink` | Entities; people |
| `--hue-amber` | `#F4B860` | `hue-amber` | Slack sources; extraction candidates |
| `--hue-teal` | `#2A9D8F` | `hue-teal` | Meetings; structured/ingested content |
| `--hue-plum` | `#7C5295` | `hue-plum` | Systems (MCP, references); archive |

Tinted fills derive from hues via `color-mix(in srgb, var(--hue-x) 18%, transparent)`
(chips, kind badges) and 22%/40% for avatar fill/border.

### Status

| Token | Value | Tailwind | Use |
|---|---|---|---|
| `--ok` | `#3F8A5B` | `ok` (alias `kelp`) | Success, connected, chosen |
| `--warn` | `#D89A3A` | `warn` (alias `gold-dark`) | Freshness warnings, drafting |
| `--bad` | `#C24A3A` | `bad` | Errors, destructive actions |
| `--warn-soft` | `color-mix(--warn 18%)` | `warn-soft` | Warn wash: the hue-tint recipe on `--warn` (Home's needs-attention dot halo) |
| `--border-warn` | `color-mix(--warn 45%)` | `warn-line` | Warn border (Home's needs-attention row) |

### Legacy name mapping

Names surviving from the "Light Editorial" palette, remapped onto the cool
neutrals so pre-redesign screens keep compiling:

| Legacy | Now points at |
|---|---|
| `surface` | `--bg` `#FFFFFF` |
| `canvas` | `--bg-2` `#F5F6F8` (**changed meaning** — see above) |
| `coral` / `coral-dark` / `coral-soft` | `--brand` / `--brand-2` / `--brand-soft` `#FFE1D9` |
| `ink-muted`, `ink-faint` | `--ink-2`, `--ink-3` |

`surface2`, `line2`, `kelp`, `tide`, `gold` and `ocean` had zero call sites and
were removed with the retheme. Use `ok`, `hue-teal`, `hue-amber`/`warn` and
`hue-plum` directly.

New code should prefer the canonical names; legacy names exist only so old
screens keep working until they are restyled.

### Spacing, radii, shadows

- **Spacing scale**: 2 / 4 / 8 / 12 / 16 / 24 / 32 / 48 / 64 / 96 / 128 px
  (`--s-0`…`--s-10`; identical to Tailwind `0.5 1 2 3 4 6 8 12 16 24 32`).
- **Radii**: named for what they wrap — `rounded-badge` (6), `rounded-row` (8),
  `rounded-field` (9, also sidebar items), `rounded-control` (10),
  `rounded-card` (12), `rounded-frame` (14), `rounded-hud` (24, floating
  meeting surfaces), `rounded-pill` (999). CSS
  `--r-badge`…`--r-hud`, `--r-full`. Prefer `rounded-pill` over Tailwind's
  built-in `rounded-full`, which is a different, unnamed 9999px value that
  happens to look the same.
  The legacy `r1`…`r5` scale is remapped onto this (6 / 8 / 12 / 14 / 14) — the
  cream system's 16px step has no in-app equivalent. `rounded-hud` (24) is the
  exception, reserved for floating meeting surfaces.
- **Shadows**: flatter than the cream system — 1px borders do the separating,
  and shadow only lifts a row or floats a surface.
  - `shadow-row` `0 1px 2px rgba(20,22,26,.06)` — active sidebar item, lifted row
  - `shadow-card` `0 1px 2px rgba(20,22,26,.05)` — cards
  - `shadow-pop` `0 12px 28px -12px rgba(20,22,26,.16)` — popovers and menus
  - `shadow-frame` `0 28px 56px -20px rgba(20,22,26,.22)` — modals, screen frames
  - `shadow-overlay` `0 22px 54px -16px rgba(0,0,0,.5)` — over dark backdrops
  - legacy `shadow-1/2/3` map onto `row` / `pop` / `frame`

  `shadow-pop` is the one level not in the handoff: its four-shadow scale has no
  step for an in-app dropdown (`frame` is sized for the 1440×900 mock frames),
  so it is interpolated between `card` and `frame`.

## Motion

Motion shows where content came from and where it went. It never decorates.

**Rules.**

- Every enter uses `--ease-out`.
- Only the inline cite chip uses `--ease-pop`, the one overshoot in the app.
- Only the shared `DetailPanel` and its mobile sheet slide. Nothing else
  moves in from off screen, and pages do not transition.
- Every animation has a reduced-motion end state. Under
  `prefers-reduced-motion: reduce`, a global rule in `globals.css` sets every
  animation and transition duration and delay to 0, so content lands at its
  end state at once. Components that loop (the shimmer, the swarm mark) also
  turn the loop off through `useReducedMotion()`.
- Controls (pills, rows, buttons, chips, fields) change colour, border,
  shadow and opacity through `--transition-control` and never through `transition: all`.

**Durations.** CSS `--dur-*`, Tailwind `duration-*`, `DURATION_MS` in
`web/lib/motion.ts`.

| Token | ms | Use |
|---|---|---|
| `fast` | 150 | Control hover and focus (`--transition-control`) |
| `pop` | 180 | Popovers, the cite chip scale |
| `exit` | 200 | Panel and sheet exits |
| `enter` | 220 | Small enters, such as a stage line or an error note |
| `collapse` | 240 | Expand and collapse |
| `drawer` | 280 | Panel and sheet enters |
| `reveal` | 320 | Answer blocks and cards revealing |
| `settle` | 380 | A container settling to its final size |
| `slow` | 500 | Rare, slow fades |

**Easings.** CSS `--ease-*`, Tailwind `ease-out` / `ease-exit` / `ease-pop`,
`EASE` in `web/lib/motion.ts`.

| Token | Curve | Use |
|---|---|---|
| `--ease` | `ease` | Control transitions |
| `--ease-out` | `cubic-bezier(.22,1,.36,1)` | Every enter |
| `--ease-exit` | `cubic-bezier(.4,0,1,1)` | Panel, sheet and popover exits |
| `--ease-pop` | `cubic-bezier(.34,1.32,.64,1)` | The inline cite chip only |

**Stagger.** `STAGGER_MS` in `web/lib/motion.ts`: block 40, cite 30, card 50,
word 60.

**Keyframes** in `globals.css`, with Tailwind `animate-*` utilities:
`cs-shimmer` (`animate-shimmer`, the progress label), `cs-stage-in`
(`animate-stage-in`), `cs-pop-in` (`animate-pop-in`), `cs-skeleton`
(`animate-skeleton`).

**The `motion` package.** Import it only through `web/lib/motion.ts`, which
re-exports `LazyMotion`, `domAnimation`, `m`, `AnimatePresence`,
`MotionConfig` and `useReducedMotion`. Render `m.*` elements inside
`<LazyMotion features={domAnimation}>`, not `motion.*`, to keep the bundle
small. Read durations and easings from `DURATION_MS` and `EASE`.
`web/lib/motion.test.ts` fails when they drift from `globals.css`.

**Chat tokens.** `--thread-max` (720, `max-w-thread`), `--composer-max-h`
(168, `max-h-composer`), `--chat-header-h` (48, `h-chat-header`),
`--r-cite` (5, `rounded-cite`), `--r-mark` (3, `rounded-mark`),
`--on-brand` (text on a filled brand control, `text-on-brand`),
`--focus-ring` (the 22% brand ring, `ring-focus-ring`), the display roles
`--type-greeting` 44/1.08, `--type-question` 28/1.15 and
`--type-detail-title` 26/1.15 (`text-greeting`, `text-question`,
`text-detail-title`), and `--tracking-eyebrow-wide` .18em
(`tracking-eyebrow-wide`). The scrim behind a panel is the existing `--scrim`.

**Home launchpad tokens.** `--type-ask` 16.5/1.5 sans (`text-ask`), the Home
Ask box, between the composer (15) and the streamed answer (16). The "new
since your last visit" dot pulses on `cs-pulse` (`animate-new-dot`) and the
kit `Skeleton` on `cs-skeleton` (`animate-skeleton`); both stop under
`motion-reduce`.

## Typography

| Family | Stack | Voice |
|---|---|---|
| `font-display` | Instrument Serif (400 only, normal + italic) | The **human voice**: page titles, questions being asked, the featured thread name. Italic + `--brand` for the emphasized word in a heading (`SecHead`'s `italicWord`). Never bold — the face has one weight. |
| `font-sans` | Geist | The **machine voice**: UI labels, body copy, synthesized answers, buttons. |
| `font-mono` | JetBrains Mono | **Provenance**: timestamps, counts, source paths, kind labels, anything the system asserts as fact. 10–12px, uppercase + `0.18em` tracking for labels (`.lbl`). |

Rule of thumb: if a person said/asked it → serif (often italic); if the
product is answering → sans; if it's metadata/evidence → mono.

**UI text roles.** The sans and mono sizes under the display roles, as
`--type-*` in `globals.css` and Tailwind `text-*`. Screens use these rather
than a pixel size. The `text-pane*` sizes are the Teams/Outlook add-in panes'
compact scale and are not for app screens.

| Token | Size / leading | Use |
|---|---|---|
| `--type-lead` (`text-lead`) | 13.5 / 1.6 sans | A lead paragraph under a page title (Home's empty-ocean line, `PageHead`'s lead) |
| `--type-item` (`text-item`) | 13 / 1.5 sans | An item's title in a list, checklist or notice row, usually `font-medium` |
| `--type-note` (`text-note`) | 12.5 / 1.55 sans | Secondary copy: hints, notes, an empty section's text |
| `--type-micro` (`text-micro`) | 10 / 1.5 mono | Mono counts and markers (with `font-mono`), as `Mono` draws them |

**Accenting a word.** `PageTitle`'s `italicWord` accents the first match in a
string title. When the word could also occur earlier (Home's greeting: a
first name like "Go" inside "Good morning"), pass the title as nodes and wrap
the word in `TitleAccent`.

Body text: 16px / 1.55. `::selection` is brand-on-bg.

## Component inventory

### CSS classes (`web/app/globals.css`)

| Class | Use |
|---|---|
| `.lbl` / `.label` | Mono 10px uppercase tracked micro-label |
| `.pill` | Mono metadata pill (`bg-2` fill, `line` border) |
| `.cite` | Numbered citation chip — `brand-3` bg, `brand-2` text, hover → solid brand/white |
| `.field` / `.cs-input` | Text input; focus = brand border + `0 0 0 3px var(--focus-ring)` ring |
| `.btn` + `.btn-primary`(`.btn-coral`) / `.btn-secondary` / `.btn-ghost` / `.btn-danger` | Buttons: brand solid / bg+line border / transparent / `bad` outline |
| `.card`, `.panel` | Bordered surfaces on `--bg`, radius 12 / 16 |
| `.kind` + `.k-entity` `.k-decision` `.k-thread` `.k-reference` `.k-coral` | Node-kind badges on 18% hue tints (pink / coral / amber / plum / teal) |
| `.reef-bg` | Ambient 24px dot-grid texture |
| `.verdict` + `.verdict-ok` `.verdict-warn` `.verdict-bad` `.verdict-muted` | Status word + glyph on an 18% status tint, 40% status border, ink text (AA on every tint). The tone colours only the glyph and border; the word carries the meaning. Jev citation verdicts, superseded facts, ended relationships |
| `.cite-flag` | Glyph appended inside a `.cite` chip for a citation Jev flagged (unsupported `?`, contradicted `≠`, not in source `!`). Pair with a `.verdict-*` tone class; the chip's accessible name spells the verdict out |

### React kit (`web/components/ui/`)

| Component | Props | Use |
|---|---|---|
| `Badge` | `tone` (brand · accent · ok · warn · bad · ink · plum · amber · teal), `outline`, `quiet` | Solid/outline status and type badges. `quiet` is the toneless outline (`line` border, `ink-3` text) for a label that doesn't alert, e.g. Beta |
| `Chip` | `hue?` | Tinted mono chip (18% hue mix; brand-3 when hue omitted) — scopes, freshness |
| `Avatar` | `name`, `size`, `hue?` | First-letter circle, 22% hue fill / 40% hue border; hue derived from name if omitted |
| `DotMandala` | `size`, `mono`, `color`, `density`, `rotate` | The swarm mark — 5 rings (1/6/10/14/18 dots), hue-cycled |
| `Wordmark` | `size`, `mono`, `color` | `coral` + mandala + `swarm`, sans 600, −0.045em |
| `SecHead` | `num`, `title`, `italicWord?`, `lead?` | 200px/1fr section header: mono number column + display title with italic brand word |
| `PageHead`, `Empty`, `Loading`, `ErrorNote` | — | Page-level scaffolding (pre-existing, retokenized) |
| `Verdict` | `tone` (ok · warn · bad · muted), `glyph`, `title?` | `.verdict` as a component; `title` becomes the tooltip and a screen-reader description |
| `Notice` | `tone` (ok · warn), `action?` | Full-sentence banner for a confirmation (`ok`) or a currently-blocking state the reader must act on (`warn`). `ErrorNote` is the failure counterpart |
| `Meter` | `value`, `max`, `tone`, `label`, `indeterminate` | 6px usage/progress bar. `indeterminate` slides a segment along the track for unknown totals and holds still under reduced motion; it drops `aria-valuenow` |
| `Seg` | `options`, `value`, `onChange` | Pill segmented control (tablist) |
| `Toggle` | `checked`, `onChange`, `label?`, `hint?` | On/off switch (`role="switch"`) with optional label and hint |
| `Modal` | `open`, `onClose`, `title`, `description?`, `header?`, `display?`, `footer?`, `width` | Dialog with focus trap. `header` is a strip above the title (a `StepRail`); `display` sets the title in `font-display`. Below `sm` it is a bottom sheet (full width, `rounded-t-frame`) |
| `ListRow` | `onClick?`, `href?`, `active?`, `roomy?` | One row of a bordered list: a link with `href` (navigation), a button with `onClick`. `roomy` is the launchpad density (11/14px padding, 12px gap) |
| `Checkbox` | `checked`, `onChange`, `label?`, `ariaLabel?` | 15px box, `ink` fill with a ✓. Native checkbox underneath. No indeterminate state: "select all visible" is a ghost button |
| `OptionTile` | `name`, `value`, `checked`, `onChange`, `title`, `hint?` | One choice in a pick-one group. Selected = `brand-soft` fill + `brand` border. Native radio underneath; wrap the group in `role="radiogroup"` |
| `CopyPill` | `value`, `children?`, `onCopied?` | Chip-shaped pill that is itself the copy button; label goes `copy` → `copied ✓` for 1.8s; `onCopied` runs after a successful copy only |
| `Sparkline` | `values`, `labels?`, `height` | Seven day-bars in teal: `.spark-bar` for past days, full `hue-teal` for today, `line` for an empty day |
| `StepRail` | `steps`, `currentIndex`, `compact?` | Numbered step progress (done ✓ ok, current ink, upcoming outline). `compact` labels only the current step |
| `SwarmStream` | `lanes`, `gap` | Canvas stream of dots flowing into a DotMandala. **The one motion exception** (see below) |
| `Card` | `quiet?`, `flush?`, `emphasis?` | Surface. `emphasis` swaps the line border for an `ink` one: "this one" (the connector card onboarding recommends) |
| `buttonClasses` | `variant?`, `size?`, `full?`, `className?` | The `Button` classes as a string, for an anchor that must look like a button (navigation is a link, never a `<button>` inside a `<Link>`). `Button` itself uses it |

#### Motion

The system is still: transitions are the standard 0.15s colour/border change,
plus the `cs-pulse` unread dot. `SwarmStream` is the one exception, used only on
the connectors page's MCP band. It reads the `--hue-*` tokens, draws a single
static frame under `prefers-reduced-motion`, and cancels its animation frame on
unmount. `Meter indeterminate` also moves, but only as a progress indicator, and
stops under reduced motion. Don't add a third.

The shared answer renderer (`packages/answer-ui`) has the same mark as
`EvidenceItem.check` (`.csau-check-*`), mapped to `--ok` / `--warn` / `--bad`
through `--csau-ok`, `--csau-warning` and `--csau-bad` in `globals.css`.

`Hue` and `HUES` are exported for anything that needs to cycle or derive hues.

## Adding to the system

1. Propose the token/component in the prototype HTML first (it stays the
   source of truth).
2. Mirror it in `tailwind.config.ts` + `globals.css` `:root`.
3. Document it here.
4. Only then use it in screens.
