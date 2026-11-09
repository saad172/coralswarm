/* Citation chip — the numbered coral mark that follows a claim in any
   cited answer. One of the few places coral is allowed to carry a
   surface rather than just a dot (see the v3 handoff: coral is an
   accent only — dots, citation chips, the highlight badge, focus
   rings, meters). 17px square so it sits on a 14px/1.7 line without
   changing the leading. */

export function CiteChip({ n }: { n: number }) {
  return (
    <span
      aria-label={`Citation ${n}`}
      style={{
        display: "inline-flex",
        alignItems: "center",
        justifyContent: "center",
        width: 17,
        height: 17,
        flex: "none",
        borderRadius: "var(--r-badge)",
        background: "var(--brand-soft)",
        color: "var(--brand-2)",
        font: "500 9.5px/1 var(--mono)",
      }}
    >
      {n}
    </span>
  );
}

/** The chip as it appears inline inside typed answer text.
 *
 * `data-unit` marks it atomic for the typing effect (prepUnits keeps
 * it whole instead of splitting it into characters), and the -3px
 * baseline nudge is what the prototype applied at every call site. */
export function InlineCite({ n }: { n: number }) {
  return (
    <span data-unit style={{ display: "inline-flex", verticalAlign: -3 }}>
      <CiteChip n={n} />
    </span>
  );
}
