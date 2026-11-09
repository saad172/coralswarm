import Link from "next/link";
import type { ReactNode, CSSProperties } from "react";
import { WordmarkPrimary } from "./logos";

/* Shared chrome + typography for the long-form legal pages (/privacy,
 * /terms). Deliberately its own header/footer rather than <SiteNav>/
 * <SiteFooter> — those are single-page-app components (their links are
 * in-page "#anchor" scrolls tied to app/page.tsx's sections, and they're
 * "use client" for the smooth-scroll behavior). A legal page is a plain
 * static route with exactly one piece of navigation that matters: back
 * to the marketing home.
 *
 * No client-side JS at all here — these are plain server components, so
 * `app/privacy/page.tsx` / `app/terms/page.tsx` can export `metadata`
 * (which only works in a non-"use client" module) and the static export
 * (`output: "export"`) has nothing to hydrate on these routes.
 */

const h2Style: CSSProperties = {
  font: "400 21px/1.35 var(--display)",
  letterSpacing: "var(--tracking-title)",
  color: "var(--ink)",
  margin: "36px 0 12px",
};

const h3Style: CSSProperties = {
  font: "600 13.5px/1.4 var(--sans)",
  color: "var(--ink)",
  margin: "22px 0 8px",
};

const pStyle: CSSProperties = {
  font: "400 14.5px/1.75 var(--sans)",
  color: "var(--ink-2)",
  margin: "0 0 14px",
  textWrap: "pretty",
};

const ulStyle: CSSProperties = {
  margin: "0 0 14px",
  padding: "0 0 0 20px",
  display: "flex",
  flexDirection: "column",
  gap: 8,
};

const liStyle: CSSProperties = { ...pStyle, margin: 0 };

type Block =
  | { kind: "h2"; text: string }
  | { kind: "h3"; text: string }
  | { kind: "p"; text: ReactNode }
  | { kind: "ul"; items: ReactNode[] };

export function h2(text: string): Block {
  return { kind: "h2", text };
}
export function h3(text: string): Block {
  return { kind: "h3", text };
}
export function p(text: ReactNode): Block {
  return { kind: "p", text };
}
export function ul(items: ReactNode[]): Block {
  return { kind: "ul", items };
}

export function LegalProse({ blocks }: { blocks: Block[] }) {
  return (
    <div>
      {blocks.map((block, i) => {
        if (block.kind === "h2") {
          return (
            <h2 key={i} style={h2Style}>
              {block.text}
            </h2>
          );
        }
        if (block.kind === "h3") {
          return (
            <h3 key={i} style={h3Style}>
              {block.text}
            </h3>
          );
        }
        if (block.kind === "ul") {
          return (
            <ul key={i} style={ulStyle}>
              {block.items.map((item, j) => (
                <li key={j} style={liStyle}>
                  {item}
                </li>
              ))}
            </ul>
          );
        }
        return (
          <p key={i} style={pStyle}>
            {block.text}
          </p>
        );
      })}
    </div>
  );
}

export function LegalPage({
  eyebrow,
  title,
  updated,
  children,
}: {
  eyebrow: string;
  title: string;
  updated: string;
  children: ReactNode;
}) {
  return (
    <>
      <header style={{ position: "sticky", top: 0, zIndex: 80, background: "var(--bg)", borderBottom: "1px solid var(--line)" }}>
        <div
          className="cs-rail cs-rail-inner"
          style={{ height: 58, padding: "0 28px", display: "flex", alignItems: "center", gap: 16 }}
        >
          <Link
            href="/"
            aria-label="CoralSwarm — back to home"
            style={{ color: "var(--ink)", display: "flex", alignItems: "center" }}
          >
            <WordmarkPrimary size={17} />
          </Link>
          <Link href="/" className="cs-navlink" style={{ marginLeft: "auto", font: "500 13px/1 var(--sans)" }}>
            ← Back to coralswarm.example
          </Link>
        </div>
      </header>

      <main style={{ background: "var(--bg-2)", minHeight: "60vh" }}>
        <div className="cs-band" style={{ maxWidth: 760, margin: "0 auto", padding: "56px 28px 96px" }}>
          <div className="cs-eyebrow">{eyebrow}</div>
          <h1
            style={{
              font: "400 clamp(30px,4vw,42px)/1.15 var(--display)",
              letterSpacing: "var(--tracking-title)",
              color: "var(--ink)",
              margin: "14px 0 8px",
            }}
          >
            {title}
          </h1>
          <div style={{ font: "400 12px/1.6 var(--mono)", color: "var(--ink-3)", marginBottom: 28 }}>
            Last updated: {updated}
          </div>

          <div
            style={{
              background: "var(--bg)",
              border: "1px solid var(--line)",
              borderRadius: "var(--r-card)",
              boxShadow: "var(--shadow-card)",
              padding: "36px 40px",
            }}
          >
            {children}
          </div>
        </div>
      </main>

      <footer style={{ background: "var(--bg-2)", borderTop: "1px solid var(--line)" }}>
        <div
          className="cs-rail cs-rail-inner"
          style={{ padding: "24px 28px", display: "flex", flexWrap: "wrap", alignItems: "center", gap: "12px 24px" }}
        >
          <Link href="/" aria-label="CoralSwarm — back to top" style={{ color: "var(--ink)", display: "flex" }}>
            <WordmarkPrimary size={14} />
          </Link>
          <nav style={{ display: "flex", flexWrap: "wrap", gap: 16, font: "400 12.5px/1 var(--sans)" }}>
            <Link href="/privacy" className="cs-footlink">Privacy</Link>
            <Link href="/terms" className="cs-footlink">Terms</Link>
            <Link href="/support" className="cs-footlink">Support</Link>
          </nav>
          <span style={{ marginLeft: "auto", font: "var(--type-mono-meta)", fontFamily: "var(--mono)", color: "var(--ink-3)" }}>
            pre-launch · © 2026 coralswarm
          </span>
        </div>
      </footer>
    </>
  );
}
