import { FONT_MONO, sourceTypeColor, type Tokens } from "../theme";

interface DotProps {
  t: Tokens;
  sourceType: string | undefined | null;
  size?: number;
}

export function SourceTypeDot({ t, sourceType, size = 5 }: DotProps) {
  return (
    <span
      style={{
        display: "inline-block",
        flex: "none",
        width: size,
        height: size,
        borderRadius: 999,
        background: sourceTypeColor(t, sourceType),
      }}
    />
  );
}

interface LabelProps extends DotProps {
  label: string;
}

/** "● MEETING · PLATFORM SYNC · FEB 12" mono provenance line, exactly the
 *  shape every source-card/timeline row in the artboards uses. `label`
 *  already carries everything after the dot (source type + context +
 *  date), pre-joined by the caller since the join punctuation/casing varies
 *  slightly between artboards (search results vs. cite-chip source cards). */
export function SourceTypeLabel({ t, sourceType, label, size }: LabelProps) {
  return (
    <span
      style={{
        display: "flex",
        alignItems: "center",
        gap: 6,
        fontFamily: FONT_MONO,
        fontSize: 10,
        color: t.muted,
        textTransform: "uppercase",
        letterSpacing: "0.04em",
      }}
    >
      <SourceTypeDot t={t} sourceType={sourceType} size={size} />
      {label}
    </span>
  );
}
