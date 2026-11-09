import { WordmarkPrimary } from "./logos";

/* Footer.
 *
 * Every link points at a distinct destination — no aliasing. This used
 * to be two collisions ("Product"/"Connectors" both #how, "Security"/
 * "Pricing" both #pricing) because two anchors the design called for
 * simply didn't exist on the page: #ask and #agents were never
 * `id`'d, and the security guarantees had no anchor of their own once
 * v4 folded them into the pricing header. All four now exist —
 * SectionAsk already shipped id="ask", SectionAgents and the security
 * block in SectionPricing got theirs in this pass — so every label
 * below links to the thing it names instead of its nearest neighbour.
 *
 * /privacy and /terms are provided by app/privacy and app/terms
 * (added alongside this nav/SEO polish in the same PR). */

const FOOTER_LINKS = [
  { href: "#how", label: "Product" },
  { href: "#ask", label: "Ask" },
  { href: "#agents", label: "Agents" },
  { href: "#security", label: "Security" },
  { href: "#pricing", label: "Pricing" },
  { href: "#contact", label: "Contact" },
  { href: "/support", label: "Support" },
  { href: "/privacy", label: "Privacy" },
  { href: "/terms", label: "Terms" },
];

export function SiteFooter() {
  return (
    <footer style={{ background: "var(--bg-2)", borderTop: "1px solid var(--line)" }}>
      <div
        className="cs-rail cs-rail-inner"
        style={{
          padding: "36px 28px",
          display: "flex",
          flexWrap: "wrap",
          alignItems: "center",
          gap: "16px 28px",
        }}
      >
        <a href="#top" aria-label="CoralSwarm — back to top" style={{ color: "var(--ink)", display: "flex" }}>
          <WordmarkPrimary size={14} />
        </a>

        <nav style={{ display: "flex", flexWrap: "wrap", gap: 16, font: "400 12.5px/1 var(--sans)" }}>
          {FOOTER_LINKS.map((l) => (
            <a key={l.label} href={l.href} className="cs-footlink">
              {l.label}
            </a>
          ))}
        </nav>

        <span
          style={{
            marginLeft: "auto",
            font: "var(--type-mono-meta)",
            fontFamily: "var(--mono)",
            color: "var(--ink-3)",
          }}
        >
          pre-launch · looking for design partners · © 2026 coralswarm
        </span>
      </div>
    </footer>
  );
}
