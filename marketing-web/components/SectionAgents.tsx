import { DotMandala } from "./logos";

/* Block 5 — agents / MCP. The page's one dark band, and in the v4 cut
   it is a short two-column band rather than a full screen. All ivory
   text is rgba(244,238,230,…) at .9 / .8 / .72 / .6 / .5.

   id="agents" — added so nav/footer can link here directly instead of
   this band being an orphan section nothing points at. */

const COLUMNS = [
  {
    title: "persistent context",
    body: "Sessions end; the reef doesn't. No re-priming, no context docs pasted into prompts.",
  },
  {
    title: "team context, not chat history",
    body: "It answers from what the team knows — decisions, threads, references — not one user's transcripts.",
  },
  {
    title: "cited and scoped",
    body: "Every answer carries citations; a token scoped to one ocean bounds what the agent can see.",
  },
];

export function SectionAgents() {
  return (
    <section id="agents" style={{ background: "var(--ink)", color: "var(--overlay-ink)" }}>
      <div className="cs-band-dark">
        <div className="cs-grid-auto-320">
          <div>
            <div data-reveal className="cs-eyebrow cs-eyebrow-dark">agents · mcp</div>
            <h2
              data-reveal
              data-delay="80"
              style={{
                font: "400 clamp(26px,3.2vw,34px)/1.25 var(--display)",
                letterSpacing: "var(--tracking-title)",
                margin: "14px 0 0",
              }}
            >
              Agents forget. The reef doesn&apos;t.
            </h2>
            <p
              data-reveal
              data-delay="160"
              style={{
                font: "400 16px/1.65 var(--display)",
                color: "rgba(244,238,230,.72)",
                margin: "14px 0 0",
                maxWidth: 520,
                textWrap: "pretty",
              }}
            >
              An agent&apos;s context dies with its session. Plug it into the reef over MCP and every
              session opens already knowing what your team knows.
            </p>

            <div
              data-reveal
              data-delay="220"
              style={{
                marginTop: 24,
                border: "1px solid rgba(244,238,230,.25)",
                borderRadius: "var(--r-full)",
                padding: "9px 16px",
                display: "inline-flex",
                alignItems: "center",
                gap: 10,
              }}
            >
              <DotMandala size={18} mono color="var(--overlay-ink)" density={0.6} />
              <span
                style={{
                  font: "500 11px/1 var(--mono)",
                  letterSpacing: ".06em",
                  color: "rgba(244,238,230,.8)",
                }}
              >
                same answer, same citations — whoever&apos;s asking
              </span>
            </div>
          </div>

          <div data-reveal data-delay="240" style={{ display: "flex", flexDirection: "column", gap: 18 }}>
            {COLUMNS.map((c) => (
              <div key={c.title} style={{ borderTop: "1px solid rgba(244,238,230,.25)", paddingTop: 14 }}>
                <div style={{ font: "600 11px/1 var(--mono)", letterSpacing: ".08em", color: "rgba(244,238,230,.9)" }}>
                  {c.title}
                </div>
                <div style={{ font: "400 11.5px/1.8 var(--mono)", color: "rgba(244,238,230,.6)", marginTop: 8 }}>
                  {c.body}
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </section>
  );
}
