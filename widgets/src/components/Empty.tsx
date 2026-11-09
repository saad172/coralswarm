import { FONT_MONO, FONT_SERIF, type Tokens } from "../theme";

interface EmptyProps {
  t: Tokens;
  title?: string;
  body?: string;
  /** Mono footer line, e.g. "searched 3 oceans · 0 corals · 0.4s". */
  meta?: string;
}

/** Empty.dc.html's dashed no-results panel — used inside the Card shell by
 *  every template's zero-results case (ask_ocean no_answer mode,
 *  search_atoms empty results, get_reef not found, recent_activity no
 *  events). */
export function Empty({
  t,
  title = "Nothing in your ocean matches that yet.",
  body = "Try a person, a channel, or a wider date range. Corals settle a few minutes after capture.",
  meta,
}: EmptyProps) {
  return (
    <div
      style={{
        margin: 14,
        border: `1px dashed ${t.line}`,
        borderRadius: 11,
        padding: "24px 22px",
        display: "flex",
        flexDirection: "column",
        gap: 6,
        alignItems: "flex-start",
      }}
    >
      <div style={{ fontFamily: FONT_SERIF, fontStyle: "italic", fontWeight: 400, fontSize: 19, lineHeight: 1.3, color: t.ink }}>
        {title}
      </div>
      <div style={{ fontSize: 13.5, lineHeight: 1.5, color: t.secondary }}>{body}</div>
      {meta && (
        <div style={{ marginTop: 6, fontFamily: FONT_MONO, fontSize: 11, color: t.secondary, letterSpacing: "0.04em", textTransform: "uppercase" }}>
          {meta}
        </div>
      )}
    </div>
  );
}
