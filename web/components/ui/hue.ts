/* The five-hue family colours *kinds of knowledge*, not moods
   (docs/design/DESIGN_SYSTEM.md):

     coral = decisions, the brand itself
     pink  = entities, people
     amber = sources, extraction candidates
     teal  = meetings, structured/ingested content
     plum  = systems (MCP, references), archive

   Unchanged by the 2026-07 redesign, which is why every hue-keyed component
   carried over without edits. */

export type Hue = "coral" | "pink" | "amber" | "teal" | "plum";

export const HUES: Hue[] = ["coral", "pink", "amber", "teal", "plum"];

/** CSS variable reference for a hue — resolves against globals.css `:root`. */
export const hueVar = (h: Hue) => `var(--hue-${h})`;

/** Tinted fill for chips and kind badges. The redesign's tints are 12–18% of
 *  the hue over the surface; 18% is the default the kit and `.k-*` classes
 *  share, so a chip and a kind badge of the same hue read identically. */
export const hueTint = (h: Hue, pct = 18) =>
  `color-mix(in srgb, ${hueVar(h)} ${pct}%, transparent)`;

/** Stable hue for a name, so the same person keeps the same colour across
 *  avatars, dots and rows. Deterministic on the first code point — arbitrary,
 *  but consistent, which is the only property that matters here.
 *
 *  Tolerates null/undefined deliberately. This is called from render paths all
 *  over the app, usually on a name that came off the wire, and a missing one
 *  should degrade to a colour — not take the whole screen down with it. It did
 *  exactly that once, when a mistyped API field made `name` undefined. */
export const hueForName = (name: string | null | undefined): Hue =>
  HUES[(name?.codePointAt(0) ?? 0) % HUES.length];

/** The one colour for a workspace, everywhere it gets a dot: the composer
 *  picker, chat lists, workspace cards, usage rows. Hashed from the workspace
 *  ID so it never shifts when the list reorders, a workspace is renamed, or a
 *  screen shows a different subset. Position- and name-keyed hues used to
 *  give one workspace three colours across three screens. */
export function workspaceHue(id: string | null | undefined): Hue {
  let h = 0;
  const key = id ?? "";
  for (let i = 0; i < key.length; i++) h = (h * 31 + key.charCodeAt(i)) >>> 0;
  return HUES[h % HUES.length];
}
