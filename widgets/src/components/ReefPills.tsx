import { FONT_MONO, reefTypeStyle, type Tokens } from "../theme";
import type { ReefType } from "../types";

interface ReefPillProps {
  t: Tokens;
  type: ReefType | string;
  title: string;
  onClick?: () => void;
}

/** "◆ decision  Rate limiting for the public API" — the "settled into"
 *  pill from SourceExpanded.dc.html. */
export function ReefPill({ t, type, title, onClick }: ReefPillProps) {
  const style = reefTypeStyle(t, type);
  return (
    <div
      role={onClick ? "button" : undefined}
      tabIndex={onClick ? 0 : undefined}
      onClick={onClick}
      onKeyDown={
        onClick
          ? (e) => {
              if (e.key === "Enter" || e.key === " ") onClick();
            }
          : undefined
      }
      style={{
        display: "flex",
        alignItems: "center",
        gap: 8,
        border: `1px solid ${t.line}`,
        background: t.bg,
        borderRadius: 9,
        padding: "8px 11px",
        minWidth: 0,
        cursor: onClick ? "pointer" : "default",
      }}
    >
      <span
        style={{
          display: "inline-flex",
          alignItems: "center",
          gap: 4,
          borderRadius: 999,
          padding: "2px 8px",
          background: style.bg,
          color: style.fg,
          fontFamily: FONT_MONO,
          fontSize: 9,
          letterSpacing: "0.06em",
          textTransform: "uppercase",
          whiteSpace: "nowrap",
        }}
      >
        {style.glyph}
      </span>
      <span
        style={{
          fontSize: 12.5,
          color: t.ink,
          overflow: "hidden",
          textOverflow: "ellipsis",
          whiteSpace: "nowrap",
        }}
      >
        {title}
      </span>
    </div>
  );
}

interface RelationshipPillProps {
  t: Tokens;
  /** Humanized predicate, e.g. "supersedes" / "related to" / "works on". */
  label: string;
  title: string;
}

/** A generic relationship pill for `get_reef`'s `relationships[]` — teal
 *  accent bar matching ReefCard.dc.html's "relationships" section. See
 *  widgets/README.md "Backend fields needed": `relationships` carries
 *  generic entity/topical predicates (works_on, related_to, manages, …),
 *  not a dedicated decision-supersession edge, so `label` is whatever
 *  predicate the ocean actually recorded rather than the artboard's fixed
 *  "supersedes"/"related" vocabulary. */
export function RelationshipPill({ t, label, title }: RelationshipPillProps) {
  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        gap: 8,
        border: `1px solid ${t.line}`,
        borderLeft: `3px solid ${t.sourceTeal}`,
        borderRadius: 11,
        background: t.headerBg,
        padding: "9px 12px",
        minWidth: 0,
      }}
    >
      <span
        style={{
          display: "inline-flex",
          alignItems: "center",
          gap: 4,
          borderRadius: 999,
          padding: "2px 8px",
          background: "rgba(42, 157, 143, 0.14)",
          color: t.sourceTeal,
          fontFamily: FONT_MONO,
          fontSize: 9,
          letterSpacing: "0.06em",
          textTransform: "uppercase",
          whiteSpace: "nowrap",
        }}
      >
        {label}
      </span>
      <span
        style={{
          fontSize: 12.5,
          color: t.ink,
          overflow: "hidden",
          textOverflow: "ellipsis",
          whiteSpace: "nowrap",
        }}
      >
        {title}
      </span>
    </div>
  );
}
