# CoralSwarm Design System

The warm, reef-themed system behind CoralSwarm — "documentation, by the swarm."
Cream surfaces, a five-hue taxonomy, and three distinct typographic voices.

## Identity in one breath
- **Surfaces** are warm creams (`--bg`, `--bg-2`, `--bg-3`); ink is a near-black brown (`--ink`).
- **Five hues** carry meaning, never decoration: coral, pink, amber, teal, plum. Each maps to a *kind* (decision / person / candidate / meeting / system).
- **Three voices:**
  - *Serif* — `Instrument Serif`, 400 only, italic for emphasis. The human voice: questions, titles.
  - *Sans* — `Geist`, 300–700. The machine voice: synthesized answers and all UI.
  - *Mono* — `JetBrains Mono`, uppercase labels. Provenance: who · when · where · confidence.

## Using it
1. Load the tokens: `<link rel="stylesheet" href="styles.css">` — drives all color, type, spacing, radii, shadows.
2. Load components: pull `_ds_bundle.js` and read off the namespace.
   ```html
   <script src="_ds_bundle.js"></script>
   <script>
     const { Button, Badge, Card } = window.CoralSwarmDesignSystem_91cd0b;
   </script>
   ```

## Components
| Component | Purpose |
|-----------|---------|
| `Button`  | Pill button — `primary` / `ink` / `secondary` / `ghost` / `danger`, sizes `sm`/`md`/`lg`. |
| `Input`   | Pill text field — sizes, coral focus ring, invalid + disabled. |
| `Badge`   | Kind/status label — `solid` or `outline`, one of the five hues. |
| `Pill`    | Mono status pill with a hue dot (e.g. "now in private beta"). |
| `HueChip` | Five-hue taxonomy legend chip (hue + meaning + hex). |
| `CiteChip`| Inline citation chip — `soft` default, `hot` when active. |
| `Card`    | Reef card — hue-coded kind via 3px left-border + chip; provenance always shown. |
| `Nav`     | Sticky top bar — brand slot, links, CTA pill; cream + ink. |
| `Section` | Full-width surface band — cream / tinted / ink, optional dotted texture. |
| `Eyebrow` | Mono uppercase kicker label. |
| `Display` | Instrument-Serif display heading; `<em>` renders coral italic. |
| `Wordmark`/`DotMandala` | Primary lockup + the concentric dot-cluster mark. |

## Rules
- Kind is signalled by hue **border + chip**, never by tinting a surface.
- Every claim shows provenance. Mono is for metadata only.
- Never recolor outside the five hues; never stretch or box the wordmark.

## Status
Core component pass complete — 13 live components covering type, labels, surfaces,
nav, forms, and brand. Next: marketing-page + pitch-deck templates, and Foundations
cards for spacing / radii / motion.
