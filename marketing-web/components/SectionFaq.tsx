/* Block 6.5 — FAQ, sitting between pricing and contact.
 *
 * `FAQ_ITEMS` is exported and imported directly by app/layout.tsx to
 * build the FAQPage JSON-LD — Google requires that structured-data
 * Q/A text match what's visibly on the page, and importing the same
 * array guarantees that by construction instead of by two people
 * remembering to keep two copies in sync.
 *
 * Every answer below is grounded in copy that already exists
 * elsewhere on this page (Hero's subhead/description, SectionHow,
 * SectionAgents, SectionAsk's oceans list, SectionPricing's SECURITY
 * lines and PLANS) — nothing here is a new claim.
 *
 * No hooks, no "use client" — like SectionAgents/SectionPricing, this
 * is static markup; [data-reveal] is driven by Page.tsx's global
 * useReveal() observer, not a per-component effect. */

export const FAQ_ITEMS: { q: string; a: string }[] = [
  {
    q: "What is CoralSwarm?",
    a: "CoralSwarm captures the work your team is already doing — meetings, chat, email, coding sessions — and turns it into organizational memory, automatically. It splits what comes in into atoms, groups them into topics you confirm, and answers questions with citations, for people and AI agents alike.",
  },
  {
    q: "How is CoralSwarm different from Notion or Obsidian?",
    a: "Notion and Obsidian are places you write things down — the knowledge only exists if someone takes the time to file it there. CoralSwarm connects to what your team already uses and captures automatically, so nothing depends on someone remembering to write a doc, and nothing is ingested that you didn't connect.",
  },
  {
    q: "Do I have to write anything for knowledge to get captured?",
    a: "No. Meetings, chat, email, code and agent sessions connect once, and the swarm takes it from there — you never file anything yourself.",
  },
  {
    q: "Can AI agents like Claude use CoralSwarm?",
    a: "Yes — CoralSwarm has an MCP connector, so an agent's session can open already knowing what your team knows instead of starting from zero every time. Answers are cited and scoped: a token bound to one ocean limits what any agent can see, and it gets the same answer with the same citations a person asking would.",
  },
  {
    q: "Is my data private and secure?",
    a: "Yes. Every capture lands in your own private ocean and stays there until you actively promote it — nothing moves on its own. Your keys never touch our servers, so we can't read your data or recover it, and on Enterprise each tenant runs on its own private database rather than a shared one.",
  },
  {
    q: "How much does CoralSwarm cost?",
    a: "Personal Pro is $25/month for 1,500 queries; Personal Max is $100/month for 7,000 queries — both include every connector and MCP access, with a 14-day free trial at launch. Enterprise is priced per seat and quoted with your team; capture, ingestion and review are never metered, only queries are.",
  },
];

export function SectionFaq() {
  return (
    <section id="faq" style={{ background: "var(--bg)", borderTop: "1px solid var(--line)" }}>
      <div className="cs-band">
        <div style={{ maxWidth: 640 }}>
          <div data-reveal className="cs-eyebrow">faq</div>
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
            Questions.
          </h2>
        </div>

        <div data-reveal data-delay="140" style={{ marginTop: 30, maxWidth: 720 }}>
          {FAQ_ITEMS.map((item) => (
            <details key={item.q} className="cs-faq-item" style={{ borderTop: "1px solid var(--line-3)" }}>
              <summary className="cs-faq-summary">
                <h3
                  style={{
                    font: "400 18px/1.35 var(--display)",
                    letterSpacing: "var(--tracking-title)",
                    margin: 0,
                  }}
                >
                  {item.q}
                </h3>
                <span className="cs-faq-caret" aria-hidden="true">
                  <span />
                  <span />
                </span>
              </summary>
              <p
                style={{
                  font: "400 14px/1.7 var(--sans)",
                  color: "var(--ink-2)",
                  margin: "0 0 18px",
                  maxWidth: 640,
                  textWrap: "pretty",
                }}
              >
                {item.a}
              </p>
            </details>
          ))}
        </div>
      </div>
    </section>
  );
}
