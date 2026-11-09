/* Block 6 — pricing, with the security guarantees folded into the
 * header row. In v3 security was a whole section; the v4 cut reduces it
 * to three mono lines beside the pricing lede.
 *
 * That block carries its own id="security" (below) so nav/footer links
 * can point at it directly instead of aliasing #pricing — it used to
 * have no anchor of its own, which forced "Security" and "Pricing" nav
 * links to collide on the same target.
 *
 * Numbers, quotas and body copy are the design's, verbatim. The two
 * personal CTAs read "Join the waitlist" rather than the design's "Get a
 * free trial today", and the ribbon says the trial is at launch — the
 * same call that was made on v3, for the same reason: every CTA lands on
 * #contact, which is a waitlist form. There is no billing and no trial
 * to start, so the design's wording would promise something the button
 * cannot do. Revert both strings the moment billing ships. */

const SECURITY = [
  "01 · your own Postgres, not a tenant column",
  "02 · nothing leaves your ocean until you promote it",
  "03 · your keys never touch our servers — we can't read it, or recover it",
];

type Plan = {
  eyebrow: string;
  price: string;
  per?: string;
  quota: string;
  body: string;
  cta: string;
  featured?: boolean;
  quiet?: boolean;
};

const PLANS: Plan[] = [
  {
    eyebrow: "personal pro",
    price: "$25",
    per: "/ month",
    quota: "1,500 queries / month",
    body: "Your own private ocean, every connector, MCP access for your agents. Top-ups when you run dry, and a ceiling you set yourself.",
    cta: "Join the waitlist",
    featured: true,
  },
  {
    eyebrow: "personal max",
    price: "$100",
    per: "/ month",
    quota: "7,000 queries / month",
    body: "Everything in Pro at agent volume — for people whose agents ask more than they do. Same trial, top-ups and self-set ceiling.",
    cta: "Join the waitlist",
  },
  {
    eyebrow: "enterprise",
    price: "Talk to sales",
    quota: "per seat · quota set with you",
    body: "Team and org oceans on top of everyone's private one — promote flows, review queue, scoped MCP tokens, admin limits.",
    cta: "Talk to sales",
    quiet: true,
  },
];

export function SectionPricing() {
  return (
    <section id="pricing" style={{ background: "var(--bg-2)", borderTop: "1px solid var(--line)" }}>
      <div className="cs-band">
        <div
          style={{
            display: "flex",
            flexWrap: "wrap",
            gap: 36,
            alignItems: "flex-end",
            justifyContent: "space-between",
          }}
        >
          <div style={{ maxWidth: 520 }}>
            <div data-reveal className="cs-eyebrow">pricing · 14-day free trial</div>
            <h2
              data-reveal
              data-delay="80"
              style={{
                font: "400 clamp(26px,3vw,32px)/1.25 var(--display)",
                letterSpacing: "var(--tracking-title)",
                color: "var(--ink)",
                margin: "14px 0 0",
              }}
            >
              One meter: the answer.
            </h2>
            <p
              data-reveal
              data-delay="160"
              style={{
                font: "400 14.5px/1.7 var(--sans)",
                color: "var(--ink-2)",
                margin: "12px 0 0",
                textWrap: "pretty",
              }}
            >
              Capture, ingestion and review are never metered. One thing is counted: a query — asked
              by you in the app or pulled by an agent over MCP.
            </p>
          </div>

          <div
            id="security"
            data-reveal
            data-delay="200"
            style={{ display: "flex", flexDirection: "column", gap: 7, maxWidth: 400 }}
          >
            <div className="cs-eyebrow" style={{ paddingBottom: 4 }}>
              private by construction
            </div>
            {SECURITY.map((line) => (
              <div key={line} style={{ font: "400 11.5px/1.6 var(--mono)", color: "var(--ink-2)" }}>
                {line}
              </div>
            ))}
          </div>
        </div>

        <div data-reveal data-delay="140" className="cs-grid-auto-258" style={{ marginTop: 36 }}>
          {PLANS.map((p) => (
            <div
              key={p.eyebrow}
              style={{
                position: "relative",
                background: p.quiet ? "var(--bg-subtle)" : "var(--bg)",
                border: `1px solid ${p.featured ? "var(--ink-3)" : p.quiet ? "var(--line-2)" : "var(--line)"}`,
                boxShadow: p.featured ? "var(--shadow-pop)" : p.quiet ? "none" : "var(--shadow-card)",
                borderRadius: "var(--r-card)",
                padding: 22,
                display: "flex",
                flexDirection: "column",
              }}
            >
              {p.featured && (
                <div
                  style={{
                    position: "absolute",
                    top: -9,
                    left: 22,
                    display: "flex",
                    alignItems: "center",
                    gap: 6,
                    background: "var(--brand-soft)",
                    borderRadius: "var(--r-full)",
                    padding: "4px 10px",
                  }}
                >
                  <span style={{ width: 5, height: 5, borderRadius: "var(--r-full)", background: "var(--brand)", flex: "none" }} />
                  <span
                    style={{
                      font: "600 9.5px/1 var(--mono)",
                      letterSpacing: ".08em",
                      textTransform: "uppercase",
                      color: "var(--brand-2)",
                      whiteSpace: "nowrap",
                    }}
                  >
                    14 days free at launch · cancel inside it
                  </span>
                </div>
              )}

              <div className="cs-eyebrow">{p.eyebrow}</div>

              <div style={{ display: "flex", alignItems: "baseline", gap: 6, marginTop: 16 }}>
                {p.per ? (
                  <>
                    <span style={{ font: "400 32px/1 var(--mono)", color: "var(--ink)" }}>{p.price}</span>
                    <span style={{ font: "400 12px/1 var(--mono)", color: "var(--ink-3)" }}>{p.per}</span>
                  </>
                ) : (
                  <span style={{ font: "400 26px/1.2 var(--display)", color: "var(--ink)" }}>{p.price}</span>
                )}
              </div>

              <div style={{ font: "400 11.5px/1.6 var(--mono)", color: "var(--ink-3)", marginTop: 8 }}>
                {p.quota}
              </div>

              <div style={{ height: 1, background: "var(--line-3)", margin: "16px 0" }} />

              <div style={{ font: "400 12.5px/1.6 var(--sans)", color: "var(--ink-2)" }}>{p.body}</div>

              <div style={{ marginTop: "auto", paddingTop: 20 }}>
                <a
                  href="#contact"
                  className={p.featured ? "cs-btn-solid" : "cs-btn-quiet-fill"}
                  style={{
                    display: "flex",
                    borderRadius: "var(--r-row)",
                    padding: "9px 14px",
                    font: "500 13px/1 var(--sans)",
                  }}
                >
                  {p.cta}
                </a>
              </div>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}
