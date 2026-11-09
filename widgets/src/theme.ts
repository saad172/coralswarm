// Design tokens lifted verbatim from the approved artboards' literal inline
// styles (widget-design/*.dc.html) — those styles ARE the tokens, no CSS
// vars in the source of truth. Dark tokens are DERIVED (not specified by any
// artboard): same hue family, ink/canvas inverted, coral kept as-is, per the
// founder's instruction.

export type ThemeName = "light" | "dark";

export interface Tokens {
  canvasBg: string;
  bg: string;
  headerBg: string;
  sourceCardBg: string;
  ink: string;
  secondary: string;
  line: string;
  lineSoft: string;
  muted: string;
  mutedSoft: string;
  faint: string;
  coral: string;
  coralDark: string;
  coralChipBg: string;
  userBubbleBg: string;
  shadow: string;
  sourceTeal: string;
  sourceAmber: string;
  sourcePurple: string;
  sourcePink: string;
}

export const LIGHT: Tokens = {
  canvasBg: "#F5F6F8",
  bg: "#FFFFFF",
  headerBg: "#FAFBFC",
  sourceCardBg: "#FCFCFD",
  ink: "#14161A",
  secondary: "#4B5058",
  line: "#E4E6EA",
  lineSoft: "#EDEEF1",
  muted: "#8A9099",
  mutedSoft: "#A6ABB3",
  faint: "#C6CAD1",
  coral: "#F77764",
  coralDark: "#D14A38",
  coralChipBg: "#FFE1D9",
  userBubbleBg: "#F3F4F6",
  shadow: "0 1px 2px rgba(20,22,26,.05)",
  sourceTeal: "#2A9D8F",
  sourceAmber: "#F4B860",
  sourcePurple: "#7C5295",
  sourcePink: "#EB7290",
};

// Derived: canvas/ink inverted, coral family untouched so the brand accent
// reads identically in both themes.
export const DARK: Tokens = {
  canvasBg: "#0B0C0E",
  bg: "#14161A",
  headerBg: "#191B20",
  sourceCardBg: "#1B1E23",
  ink: "#F0F1F3",
  secondary: "#B7BDC6",
  line: "#2C2F35",
  lineSoft: "#24262B",
  muted: "#9AA0A9",
  mutedSoft: "#787E87",
  faint: "#565B62",
  coral: "#F77764",
  coralDark: "#FF9C8A",
  coralChipBg: "#4A2A22",
  userBubbleBg: "#1F2227",
  shadow: "0 1px 2px rgba(0,0,0,.35)",
  sourceTeal: "#39B8A8",
  sourceAmber: "#F4B860",
  sourcePurple: "#9B76BC",
  sourcePink: "#EB7290",
};

export function tokensFor(theme: ThemeName | undefined | null): Tokens {
  return theme === "dark" ? DARK : LIGHT;
}

export const FONT_SERIF = "'Instrument Serif', Georgia, 'Times New Roman', serif";
export const FONT_SANS =
  "Geist, ui-sans-serif, system-ui, -apple-system, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif";
export const FONT_MONO =
  "'JetBrains Mono', ui-monospace, SFMono-Regular, Menlo, Consolas, 'Liberation Mono', monospace";

export const GOOGLE_FONTS_HREF =
  "https://fonts.googleapis.com/css2?family=Instrument+Serif:ital@0;1&family=Geist:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap";

/** Source-type -> accent dot color (canvas.json / artboards' fixed mapping:
 *  teal / amber / purple / pink), plus a couple of sensible additions for
 *  source_type values `AtomFilterFields.source_type`'s doc comment lists
 *  that no artboard shows a dot for. Unknown types fall back to muted. */
export function sourceTypeColor(t: Tokens, sourceType: string | undefined | null): string {
  switch (sourceType) {
    case "meeting":
    case "transcript":
    case "zoom":
    case "meet":
    case "teams_transcript":
      return t.sourceTeal;
    case "slack":
    case "chat":
    case "teams_chat":
    case "teams_channel":
      return t.sourceAmber;
    case "coding_session":
    case "mcp":
      return t.sourcePurple;
    case "email":
      return t.sourcePink;
    case "note":
    case "document":
    case "notion":
    case "gdrive":
      return t.coral;
    default:
      return t.muted;
  }
}

/** Reef-type hue mapping — matches `web/app/(app)/coral/page.tsx`'s
 *  `REEF_HUE` (DESIGN_SYSTEM.md: entity→pink, decision→coral, thread→amber,
 *  reference→plum), plus the "◆ decision" chip glyph the artboards use. */
export interface ReefTypeStyle {
  fg: string;
  bg: string;
  glyph: string;
}

export function reefTypeStyle(t: Tokens, type: string): ReefTypeStyle {
  switch (type) {
    case "decision":
      return { fg: t.coralDark, bg: t.coralChipBg, glyph: "◆ decision" };
    case "thread":
      return { fg: "#D89A3A", bg: withAlpha(t.sourceAmber, 0.2), glyph: "thread" };
    case "entity":
      return { fg: t.sourcePink, bg: withAlpha(t.sourcePink, 0.16), glyph: "entity" };
    case "reference":
      return { fg: t.sourcePurple, bg: withAlpha(t.sourcePurple, 0.16), glyph: "reference" };
    default:
      return { fg: t.muted, bg: t.lineSoft, glyph: type };
  }
}

/** Cheap "color-mix"-equivalent for a hex color at a given alpha — the
 *  artboards use `color-mix(in srgb, X 16%, transparent)`, which not every
 *  widget sandbox's CSS engine supports; an rgba() string is the safe
 *  equivalent. */
function withAlpha(hex: string, alpha: number): string {
  const m = /^#([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})$/i.exec(hex);
  const rHex = m?.[1];
  const gHex = m?.[2];
  const bHex = m?.[3];
  if (!rHex || !gHex || !bHex) return hex;
  const r = parseInt(rHex, 16);
  const g = parseInt(gHex, 16);
  const b = parseInt(bHex, 16);
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}

export function sourceTypeLabel(sourceType: string | undefined | null): string {
  switch (sourceType) {
    case "coding_session":
      return "coding session";
    case "teams_transcript":
      return "teams";
    case "teams_channel":
      return "teams channel";
    case "teams_chat":
      return "teams chat";
    case "gdrive":
      return "drive";
    case undefined:
    case null:
      return "coral";
    default:
      return sourceType;
  }
}
