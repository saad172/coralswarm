import { FONT_MONO, type Tokens } from "../theme";

interface CiteChipProps {
  t: Tokens;
  index: number;
  active: boolean;
  onClick: () => void;
}

/** The small numbered coral-chip inline in ask_ocean's answer prose. Clicking
 *  it expands the matching source card INLINE from `citation_details` — see
 *  the founder's override in the task brief: never `callTool(get_atom)`
 *  here (only for a `body_omitted: true` citation, from the expanded card's
 *  own "load full coral" affordance). */
export function CiteChip({ t, index, active, onClick }: CiteChipProps) {
  return (
    <span
      role="button"
      tabIndex={0}
      onClick={onClick}
      onKeyDown={(e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          onClick();
        }
      }}
      style={{
        display: "inline-flex",
        alignItems: "center",
        justifyContent: "center",
        minWidth: 17,
        height: 17,
        margin: "0 1px",
        padding: "0 5px",
        borderRadius: 5,
        background: active ? t.coral : t.coralChipBg,
        color: active ? "#FFFFFF" : t.coralDark,
        fontFamily: FONT_MONO,
        fontSize: 10,
        fontWeight: 500,
        verticalAlign: "2px",
        cursor: "pointer",
      }}
    >
      {index}
    </span>
  );
}
